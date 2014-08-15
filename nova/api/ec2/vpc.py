# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Cloud Controller: Implementation of VPC REST API calls, which are
dispatched to other nodes via AMQP RPC. State is via distributed
datastore.
"""

from netaddr import IPNetwork

from nova.api.ec2 import ec2utils
from nova import exception
from nova.openstack.common.gettextutils import _
from nova.openstack.common import log as logging

from keystoneclient import exceptions as ke
import keystoneclient.v2_0
from nova.network import neutronv2
import time
import uuid
try:
    from collections import OrderedDict
except ImportError:
    # python 2.6 or earlier, use backport
    from ordereddict import OrderedDict

LOG = logging.getLogger(__name__)


class VpcController(object):
    """
    VpcController provides the critical dispatch between
    inbound API calls through the endpoint and messages
    sent to the other nodes.
    """
    def _get_keystone_client(self, context):
        if 'context.project_name' in self.kc:
            try:
                self.kc['context.project_name'].tenants.list()
                return self.kc['context.project_name']
            except Exception as e:
                pass

        auth_url = "http://%s:5000/v2.0" % self.kc_ip
        self.kc['context.project_name'] = keystoneclient.v2_0.client.Client(
            token=context.auth_token,
            username=context.user_name,
            tenant_name=context.project_name,
            auth_url=auth_url)

        return self.kc['context.project_name']

    def _get_tenantid_from_vpcid(self, vpc_id, context):
        try:
            kc = self._get_keystone_client(context)
            tenant_list = kc.tenants.list()
            found = False
            for tenant in tenant_list:
                if tenant.name == vpc_id:
                    tenant_id = tenant.id
                    found = True
                    break
        except Exception as e:
            raise exception.InvalidRequest('Keystone exception %s' % e)

        if not found:
            raise exception.InvalidParameterValue(message='No VPC found')

        return tenant_id

    def _get_vpcid_from_tenantid(self, tenant_id, context):
        try:
            kc = self._get_keystone_client(context)
            tenant = kc.tenants.get(tenant_id)
        except Exception as e:
            return None

        return tenant.name

    def _get_vpcid_from_context(self, context):
        try:
            kc = self._get_keystone_client(context)
            tenant_list = kc.tenants.list()
            found = False
            for tenant in tenant_list:
                if tenant.id == context.project_id:
                    vpc_id = tenant.name
                    found = True
                    break
        except Exception as e:
            raise exception.InvalidRequest('Keystone exception %s' % e)

        if not found:
            raise exception.InvalidParameterValue(err='No VPC found')

        return vpc_id

    def _find_subnet_in_vpc(self, context, cidr):
        if cidr == '0.0.0.0/0':
            return 'any'

        neutron = neutronv2.get_client(context)
        try:
            network_rsp = neutron.list_networks()
        except Exception as e:
            raise exception.InvalidRequest('Neutron list nets err %s' % e)

        for network in network_rsp['networks']:
            if network['tenant_id'] != context.project_id:
                continue
            if not network['name'].startswith('subnet-'):
                continue

            try:
                subnets_rsp = neutron.list_subnets(
                    network_id=network['id'])
                for subnet in subnets_rsp['subnets']:
                    if IPNetwork(cidr) in IPNetwork(subnet['cidr']):
                        return network['name']
            except Exception as e:
                raise exception.InvalidRequest('Neutron list subnets err %s' % e)

        raise exception.InvalidRequest('Subnet for %s not found' % cidr)

    def _create_policy_rule(self, context, kwargs):
        # parameter validation
        if not ('network_acl_id' in kwargs and 'rule_number' in kwargs and
                'protocol' in kwargs and 'egress' in kwargs and
                'cidr_block' in kwargs):
            raise exception.InvalidParameterValue(
                err="Required parameter missing")

        protocol = kwargs['protocol']
        action = 'pass'
        if kwargs['rule_action'] == 'deny':
            action = 'drop'

        # default direction: egress - false
        direction = 'ingress'
        if kwargs['egress']:
            direction = 'egress'

        # get ports
        port_range = None
        start_port = end_port = -1
        if 'port_range' in kwargs:
            port_range = kwargs.get('port_range')
            if 'from' in port_range:
                start_port = port_range['from']
            if 'to' in port_range:
                end_port = port_range['to']

        # get cidr block
        cidr_block = kwargs.get('cidr_block')
        network = self._find_subnet_in_vpc(context, cidr_block)
        cidr = {'ip_prefix': str(cidr_block.split('/')[0]),
                'ip_prefix_len': int(cidr_block.split('/')[1])}

        # vpc id
        vpc_id = self._get_vpcid_from_context(context)

        # set policy values based on direction
        direction = '>'
        if kwargs['egress']:
            rule_no = 'egress-' + str(kwargs['rule_number'])
            src_cidr = None
            src_nw = 'local'
            dst_cidr = cidr
            dst_nw = '%s:%s:%s' % ('default-domain', vpc_id, network)
            egress_start_port = start_port
            egress_end_port = end_port
            ingress_start_port = -1
            ingress_end_port = -1
        else:
            rule_no = 'ingress-' + str(kwargs['rule_number'])
            src_cidr = cidr
            src_nw = '%s:%s:%s' % ('default-domain', vpc_id, network)
            dst_cidr = None
            dst_nw = 'local'
            ingress_start_port = start_port
            ingress_end_port = end_port
            egress_start_port = -1
            egress_end_port = -1

        # create rule
        pol_dict = {'policy_rule': [{'direction': direction,
                                     'protocol': protocol,
                                     'dst_addresses': [{
                                         'security_group': None,
                                         'subnet': dst_cidr,
                                         'virtual_network': dst_nw}],
                                     'action_list': None,
                                     'rule_uuid': rule_no,
                                     'dst_ports': [
                                         {'end_port': egress_end_port,
                                          'start_port': egress_start_port}],
                                     'application': [],
                                     'action_list': {'simple_action': action},
                                     'rule_sequence': None,
                                     'src_addresses': [
                                         {'security_group': None,
                                          'subnet': src_cidr,
                                          'virtual_network': src_nw}],
                                     'src_ports': [
                                         {'end_port': ingress_end_port,
                                          'start_port': ingress_start_port}]}]}
        return pol_dict

    def _get_security_group_rule_params(self, context, kwargs):
        # set rule parameters from fuction arguments
        req = {}

        # check if protocol parameter is provided
        if not kwargs['ip_permissions'][0]['ip_protocol']:
            raise exception.InvalidParameterValue(err='No protocol specified')
        req['protocol'] = kwargs['ip_permissions'][0]['ip_protocol']

        # check if remote security group or cidr provided
        if 'groups' in kwargs['ip_permissions'][0]:
            remote_group_id = \
                kwargs['ip_permissions'][0]['groups']['1']['group_id']
            req['remote_group_id'] = self._get_group_uuid_from_group_id(
                context, remote_group_id)
        elif 'ip_ranges' in kwargs['ip_permissions'][0]:
            req['remote_ip_prefix'] = \
                kwargs['ip_permissions'][0]['ip_ranges']['1']['cidr_ip']
        else:
            raise exception.InvalidParameterValue(
                err='destination subnet or group id required')

        # check if port numbers are provided and protocol is tcp or udp
        if req['protocol'] in ['tcp', 'udp'] and \
                'to_port' in kwargs['ip_permissions'][0]:
            req['port_range_min'] = \
                kwargs['ip_permissions'][0]['from_port']
            req['port_range_max'] = kwargs['ip_permissions'][0]['to_port']

        return req

    def _get_group_uuid_from_group_id(self, context, group_id):
        # get security group uuid from security group id
        # uuid = 2cefa85e-0a14-4f7d-8d12-a93ffe054dae
        # for security group id = sg-2cefa85e
        try:
            neutron = neutronv2.get_client(context)

            groups = neutron.list_security_groups()
            foundGroup = False
            for group in groups['security_groups']:
                if (group_id == 'default' and
                        group['tenant_id'] == context.project_id) or \
                   (group_id.startswith('sg-') and
                        group['id'][:8] == group_id.split('-')[1]):
                    foundGroup = True
                    break
        except Exception as e:
                raise exception.InvalidRequest('Neutron list sg err %s' % e)

        if not foundGroup:
            raise exception.InvalidParameterValue(
                err='No group %s found' % group_id)

        return group['id']

    def _get_rule_uuid_from_params(self, context, req):
        # check if rule with specified parameter already exists or not
        try:
            neutron = neutronv2.get_client(context)
            group_rule_rsp = neutron.list_security_group_rules()
            foundRule = False

            for rule in group_rule_rsp['security_group_rules']:
                if all(item in rule.iteritems() for item in req.iteritems()):
                    foundRule = True
                    break

            if foundRule:
                return rule['id']

            return False
        except Exception as e:
            raise exception.InvalidRequest('Neutron list sg err %s' % e)

    def create_vpc(self, context, **kwargs):
        # check if cidr address mask between 16 and 28
        if int(kwargs['cidr_block'].split('/')[1]) not in range(16, 29):
            msg = 'Subnet Mask should be between 16 and 28'
            raise exception.InvalidParameterValue(err=msg)

        # create project for each vpc
        try:
            tenant_name = 'vpc-' + ('%x' % uuid.uuid4().time_low)
            kc = self._get_keystone_client(context)
            tenant_id = kc.tenants.create(tenant_name)
        except ke.ClientException as e:
            raise exception.InvalidRequest('Keystone err %s' % e)

        # create network-ipam for vpc
        cidr_block = kwargs['cidr_block']
        neutron = neutronv2.get_client(context)

        req = {'ipam': {'mgmt': {
                        'cidr_block': {
                            'ip_prefix': str(cidr_block.split('/')[0]),
                            'ip_prefix_len': int(cidr_block.split('/')[1])}},
                        'name': tenant_name,
                        'tenant_id': tenant_id.id}}
        create_ipam = True
        while create_ipam:
            try:
                neutron.create_ipam(req)
                create_ipam = False
            except Exception as e:
                ipams = neutron.list_ipams()
                for ipam in ipams['ipams']:
                    if ipam['fq_name'][1] == tenant_name:
                        create_ipam = False
                if create_ipam == True:
                    time.sleep(3)

        # create policy and associate with vpc
        self.create_network_acl(context, vpc_id=[tenant_name], default=True)

        # create default route table
        self.create_route_table(context, vpc_id=tenant_name,
                                default_route_table=True)

        return {'vpc': {'vpcId': tenant_name, 'state': 'available'}}

    def delete_vpc(self, context, **kwargs):
        vpc_id = kwargs['vpc_id']
        tenant_id = self._get_tenantid_from_vpcid(vpc_id, context)
        neutron = neutronv2.get_client(context)
        try:
            # delete default subnet
            nets = neutron.list_networks()
            for net in nets['networks']:
                if net['tenant_id'] == tenant_id:
                    neutron.delete_network(net['id'])

            # delete default route table
            routes = neutron.list_route_tables()
            for route in routes['route_tables']:
                if route['fq_name'][1] == vpc_id:
                    neutron.delete_route_table(route['id'])

            # delete default security group
            groups = neutron.list_security_groups()
            for group in groups['security_groups']:
                if (group['tenant_id'] == tenant_id and
                        group['name'] != 'default'):
                    neutron.delete_security_group(group['id'])

            # delete default network policy
            policys = neutron.list_policys()
            for pol in policys['policys']:
                if pol['fq_name'][1] == vpc_id:
                    neutron.delete_policy(pol['id'])

            # delete default-network-ipam
            ipams = neutron.list_ipams()
            for ipam in ipams['ipams']:
                if ipam['fq_name'][1] == vpc_id:
                    neutron.delete_ipam(ipam['id'])

        except Exception as e:
            raise exception.InvalidRequest('VPC delete err %s' % e)

        # delete project for the passed vpc
        try:
            kc = self._get_keystone_client(context)
            vpc_list = kc.tenants.list()
            for vpc in vpc_list:
                if vpc.name == vpc_id:
                    kc.tenants.delete(vpc)
                    del self.kc['context.project_name']
        except Exception as e:
            raise exception.InvalidRequest('VPC delete failed %s' % e)

        return {'return': 'true'}

    def describe_vpcs(self, context, **kwargs):
        if 'filter' in kwargs:
            filters = kwargs['filter']
        else:
            filters = []
        # fetch tenant list
        try:
            kc = self._get_keystone_client(context)
            tenant_list = kc.tenants.list()
        except ke.ClientException as e:
            raise exception.InvalidRequest('Keystone tenat list err %s' % e)

        # fetch ipam list
        neutron = neutronv2.get_client(context)
        try:
            ipam_rsp = neutron.list_ipams()
        except Exception as e:
            raise exception.InvalidRequest('Neutron list ipam err %s' % e)

        # list of vpcs
        vpcs = []
        for tenant in tenant_list:
            item = {}
            for ipam in ipam_rsp['ipams']:
                if not ipam['fq_name'][1].startswith('vpc-'):
                    continue
                if ipam['fq_name'][1] != tenant.name:
                    continue
                if 'vpc_id' in kwargs and tenant.name not in kwargs['vpc_id']:
                    continue

                # check for passed filters
                found = True
                for filter_entry in filters:
                    if filter_entry['name'] == 'cidrBlock':
                        cidr = filter_entry['value']['1']
                        ipam_cidr = \
                            ipam['mgmt']['cidr_block']['ip_prefix'] + \
                            '/' + str(
                                ipam['mgmt']['cidr_block']['ip_prefix_len'])
                        if cidr != ipam_cidr:
                            found = False
                    elif filter_entry['name'] == 'dhcpOptionsId':
                        dopt = filter_entry['value']['1']
                        if not ipam['mgmt']['dhcp_option_list']:
                            found = False
                            continue
                        if dopt not in ipam['mgmt']['dhcp_option_list']:
                            found = False

                if not found:
                    continue

                # populate the VPC entry in response
                item['vpcId'] = tenant.name
                item['state'] = 'available'
                if ipam['fq_name'][2].startswith('dopt-'):
                    item['dhcpOptionsId'] = ipam['fq_name'][2]
                if ipam['fq_name'][2].startswith('vpc-'):
                    if ipam['mgmt'] and ipam['mgmt']['cidr_block']:
                        item['cidrBlock'] = str(
                            ipam['mgmt']['cidr_block']['ip_prefix']) + '/' + \
                            str(ipam['mgmt']['cidr_block']['ip_prefix_len'])

            if item:
                vpcs.append(item)

        return {'vpcSet': vpcs}

    def create_dhcp_option(self, context, **kwargs):
        # create dhcp options
        dhcp_options_id = 'dopt-' + ('%x' % uuid.uuid4().time_low)
        dhcp_options = kwargs

        # get client directly from neutronclient
        neutron = neutronv2.get_client(context)
        dhcp = []
        # create a storage ipam for dhcp options
        for key, value in dhcp_options.items():
            dhcp_set = {}
            dhcp_set['dhcp_option_name'] = key
            dhcp_set['dhcp_option_value'] = value
            dhcp.append(dhcp_set)

        req = {'name': dhcp_options_id,
               'tenant_id': context.project_id,
               'mgmt': {'dhcp_option_list': {'dhcp_option': dhcp}}}
        try:
            neutron.create_ipam({'ipam': req})
        except Exception as e:
            raise exception.InvalidRequest('Neutron create ipam err %s' % e)

        return {'dhcpOptions': {'dhcpOptionsId': dhcp_options_id,
                                'dhcpConfigurationSet': ''}}

    def associate_dhcp_options(self, context, **kwargs):
        vpc_id = kwargs.get('vpc_id')[0]
        dhcp_options_id = kwargs.get('dhcp_options_id')[0]

        # get ipam list
        neutron = neutronv2.get_client(context)
        try:
            ipam_rsp = neutron.list_ipams()
        except Exception as e:
            raise exception.InvalidRequest('Neutron list ipam err %s' % e)

        # copy the dhcp options from storage ipam to vpc ipam
        foundDhcp = True if dhcp_options_id == 'default' else False
        foundVpcIpam = False
        req = {'mgmt': {}}

        for ipam in ipam_rsp['ipams']:
            if ipam['fq_name'][2] == dhcp_options_id:
                foundDhcp = True
                req['mgmt']['dhcp_option_list'] = \
                    ipam['mgmt']['dhcp_option_list']
            elif ipam['fq_name'][2] == vpc_id:
                foundVpcIpam = True
                req['mgmt']['cidr_block'] = ipam['mgmt']['cidr_block']
                vpcIpamId = ipam['id']

            if foundDhcp and foundVpcIpam:
                break

        if not (foundDhcp and foundVpcIpam):
            raise exception.InvalidParameterValue(
                err='No matching VPC or DHCP options')

        try:
            neutron.update_ipam(vpcIpamId, {'ipam': req})
        except Exception as e:
            raise exception.InvalidRequest('Neutron update ipam err %s' % e)

        return {'return': 'true'}

    def delete_dhcp_options(self, context, **kwargs):
        dhcp_options_id = kwargs.get('dhcp_options_id')[0]

        # get client directly from neutronclient
        neutron = neutronv2.get_client(context)
        try:
            ipam_rsp = neutron.list_ipams()
        except Exception as e:
            raise exception.InvalidRequest('Neutron list ipam err %s' % e)

        for ipam in ipam_rsp['ipams']:
            if ipam['fq_name'][2] == dhcp_options_id:
                try:
                    neutron.delete_ipam(ipam['id'])
                except Exception as e:
                    raise exception.InvalidRequest('Neutron delete ipam err %s' % e)

        return {'return': 'true'}

    def describe_dhcp_options(self, context, **kwargs):
        # get client directly from neutronclient
        neutron = neutronv2.get_client(context)
        try:
            ipam_rsp = neutron.list_ipams()
        except Exception as e:
            raise exception.InvalidRequest('Neutron list ipam err %s' % e)

        item_list = []
        for ipam in ipam_rsp['ipams']:
            if not ipam['fq_name'][1].startswith('vpc-'):
                continue
            if not ipam['fq_name'][2].startswith('dopt-'):
                continue

            items = []
            for options in ipam['mgmt']['dhcp_option_list']['dhcp_option']:
                item = OrderedDict()
                item['key'] = options['dhcp_option_name']
                item['valueSet'] = [{'value': options['dhcp_option_value']}]
                items.append(item)

            top_items = {}
            top_items['dhcpOptionsId'] = ipam['fq_name'][2]
            top_items['dhcpConfigurationSet'] = items
            item_list.append(top_items)

        return {'dhcpOptionsSet': list(item_list)}

    def create_subnet(self, context, **kwargs):
        vpc_id = kwargs['vpc_id']
        cidr_block = kwargs['cidr_block']
        subnet_ip_block = IPNetwork(cidr_block)

        # get project id
        tenant_id = self._get_tenantid_from_vpcid(vpc_id, context)
        neutron = neutronv2.get_client(context)

        # check if subnet cidr in vpc cidr
        try:
            ipam_rsp = neutron.list_ipams()
        except Exception as e:
            raise exception.InvalidRequest('Neutron list ipam err %s' % e)

        for ipam in ipam_rsp['ipams']:
            if ipam['fq_name'][2] == vpc_id:
                vpc_ip_block = IPNetwork(
                    ipam['mgmt']['cidr_block']['ip_prefix'] + '/' +
                    str(ipam['mgmt']['cidr_block']['ip_prefix_len']))
                first_vpc_add = vpc_ip_block[0]
                last_vpc_add = vpc_ip_block[-1]
                first_subnet_add = subnet_ip_block[0]
                last_subnet_add = subnet_ip_block[-1]
                if first_subnet_add < first_vpc_add or \
                   last_subnet_add > last_vpc_add:
                    msg = "subnet ip block not in vpc ip block"
                    raise exception.InvalidParameterValue(err=msg)
                break

        try:
            if 'default_subnet' not in kwargs:
                vn_name = 'subnet-' + ('%x' % uuid.uuid4().time_low)
            else:
                vn_name = 'subnet-default'

            # associate default network policy
            policy_l = neutron.list_policys()
            for pol in policy_l['policys']:
                if pol['fq_name'][1] == vpc_id and \
                   pol['fq_name'][2] == 'acl-default':
                    break

            # associate default route table
            route_rsp = neutron.list_route_tables()
            for route_table in route_rsp['route_tables']:
                if route_table['name'] == 'rtb-default' and \
                   route_table['fq_name'][1] == vpc_id:
                    break

            # create VN
            net_req = {'name': vn_name, 'tenant_id': tenant_id,
                       'contrail:policys': [pol['fq_name']],
                       'vpc:route_table': route_table['fq_name']}
            net_rsp = neutron.create_network({'network': net_req})
        except Exception as e:
            raise exception.InvalidRequest('Neutron create net err %s' % e)

        # set subnet for VN
        subnet_req = {'network_id': net_rsp['network']['id'],
                      'cidr': unicode(cidr_block),
                      'ip_version': 4,
                      'tenant_id': tenant_id,
                      'contrail:ipam_fq_name': ipam['fq_name']}
        try:
            neutron.create_subnet({'subnet': subnet_req})
        except Exception as e:
            raise exception.InvalidRequest('Neutron create subnet err %s' % e)

        return {'subnet': {'subnetId': vn_name, 'state': 'available',
                           'vpcId': vpc_id, 'cidrBlock': cidr_block}}

    def delete_subnet(self, context, **kwargs):
        # delete VN for the passed subnet_id
        subnet_id = kwargs['subnet_id']
        neutron = neutronv2.get_client(context)

        try:
            network_rsp = neutron.list_networks()
        except Exception as e:
            raise exception.InvalidRequest('Neutron list nets err %s' % e)

        for network in network_rsp['networks']:
            if not network['name'].startswith('subnet-'):
                continue
            if network['name'] != subnet_id:
                continue

            try:
                subnets_rsp = neutron.list_subnets(network_id=network['id'])
                for subnet in subnets_rsp['subnets']:
                    neutron.delete_subnet(subnet['id'])
                neutron.delete_network(network['id'])
            except Exception as e:
                raise exception.InvalidRequest('Neutron delete net err %s' % e)

        return {'return': 'true'}

    def describe_subnets(self, context, **kwargs):
        # fetch tenant list
        try:
            kc = self._get_keystone_client(context)
        except kc.ClientException as e:
            raise exception.InvalidRequest(e)

        # fetch network list
        vpc_id = self._get_vpcid_from_context(context)
        neutron = neutronv2.get_client(context)
        try:
            network_rsp = neutron.list_networks()
        except Exception as e:
            raise exception.InvalidRequest('Neutron list nets err %s' % e)

        # list of subnets/VNs
        subnets = []
        for network in network_rsp['networks']:
            if not network['name'].startswith('subnet-'):
                continue

            if (vpc_id != self._get_vpcid_from_tenantid(network['tenant_id'],
                                                        context)):
                continue

            try:
                subnets_rsp = neutron.list_subnets(
                    network_id=network['id'])
            except Exception as e:
                continue

            # get vpc id
            cidr_block = subnets_rsp['subnets'][0]['cidr']
            item = {'subnetId': network['name'],
                    'state': 'available',
                    'vpcId': vpc_id,
                    'cidrBlock': cidr_block}
            subnets.append(item)

        return {'subnetSet': subnets}

    def create_route_table(self, context, **kwargs):
        vpc_id = kwargs['vpc_id']
        tenant_id = self._get_tenantid_from_vpcid(vpc_id, context)
        if 'default_route_table' not in kwargs:
            route_table_id = 'rtb-' + ('%x' % uuid.uuid4().time_low)
        else:
            route_table_id = 'rtb-default'

        neutron = neutronv2.get_client(context)

        # get cidr block for given vpc
        try:
            ipam_rsp = neutron.list_ipams()
        except Exception as e:
            raise exception.InvalidRequest(e)

        for ipam in ipam_rsp['ipams']:
            if ipam['fq_name'][2] == vpc_id:
                cidr = ipam['mgmt']['cidr_block']['ip_prefix'] + '/' + \
                    str(ipam['mgmt']['cidr_block']['ip_prefix_len'])
                break

        # create local route
        route_dict = {'route': [{'prefix': cidr,
                                 'next_hop': 'local',
                                 'next_hop_type': None}]}

        # create route table
        req = {'name': route_table_id, 'tenant_id': tenant_id,
               'routes': route_dict}
        try:
            neutron.create_route_table({'route_table': req})
        except Exception as e:
            raise exception.InvalidRequest(e)

        return {'routeTable': {'route_table_id': route_table_id,
                               'vpc_id': vpc_id, 'routeSet': [{
                                   'destination_cidr_block': cidr,
                                   'gateway_id': 'local',
                                   'state': 'active'}]}}

    def associate_route_table(self, context, **kwargs):
        route_table_id = kwargs['route_table_id']
        subnet_id = kwargs['subnet_id']
        neutron = neutronv2.get_client(context)

        try:
            # find the subnet
            net_rsp = neutron.list_networks(tenant_id=context.project_id)
        except Exception as e:
            raise exception.InvalidRequest(e)

        found_net = False
        for net in net_rsp['networks']:
            if net['name'] == subnet_id:
                found_net = True
                break
        if not found_net:
            raise exception.InvalidParameterValue(err='No subnet found')

        try:
            # find the route table
            route_rsp = neutron.list_route_tables(tenant_id=context.project_id)
        except Exception as e:
            raise exception.InvalidRequest(e)

        foundRouteTable = False
        for route_table in route_rsp['route_tables']:
            if route_table['name'] == route_table_id:
                foundRouteTable = True
                break
        if not foundRouteTable:
            raise exception.InvalidParameterValue(
                err='No route table found')

        # associate route table to subnet
        net_req = {'vpc:route_table': route_table['fq_name']}
        neutron.update_network(net['id'], {'network': net_req})
        association_id = 'rtbassoc-' + net['id'][:8]
        return {'association_id': association_id}

    def disassociate_route_table(self, context, **kwargs):
        association_id = kwargs['association_id']
        neutron = neutronv2.get_client(context)

        try:
            # find the associated vn
            net_rsp = neutron.list_networks(tenant_id=context.project_id)
        except Exception as e:
            raise exception.InvalidRequest(e)

        found_net = False
        for net in net_rsp['networks']:
            if net['id'][:8] == association_id.split('-')[1]:
                found_net = True
                break
        if not found_net:
            raise exception.InvalidParameterValue(err='No subnet found')

        try:
            # find the default route table
            route_rsp = neutron.list_route_tables(tenant_id=context.project_id)
        except Exception as e:
            raise exception.InvalidRequest(e)

        foundRouteTable = False
        for route_table in route_rsp['route_tables']:
            if route_table['name'] == 'rtb-default' and \
               net['contrail:fq_name'][1] == route_table['fq_name'][1]:
                foundRouteTable = True
                break
        if not foundRouteTable:
            raise exception.InvalidParameterValue(
                err='No default route table found')

        # associate default route table to vn
        net_req = {'vpc:route_table': route_table['fq_name']}
        neutron.update_network(net['id'], {'network': net_req})
        association_id = 'rtbassoc-' + net['id'][:8]
        return {'return': 'true'}

    def replace_route_table_association(self, context, **kwargs):
        association_id = kwargs['association_id']
        route_table_id = kwargs['route_table_id']
        neutron = neutronv2.get_client(context)

        try:
            # find the associated vn
            net_rsp = neutron.list_networks(tenant_id=context.project_id)
        except Exception as e:
            raise exception.InvalidRequest(e)

        found_net = False
        for net in net_rsp['networks']:
            if net['id'][:8] == association_id.split('-')[1]:
                found_net = True
                break
        if not found_net:
            raise exception.InvalidParameterValue(err='No subnet found')

        # find the route table
        route_rsp = neutron.list_route_tables(tenant_id=context.project_id)
        foundRouteTable = False
        for route_table in route_rsp['route_tables']:
            if route_table['name'] == route_table_id:
                foundRouteTable = True
                break
        if not foundRouteTable:
            raise exception.InvalidParameterValue(
                err='No route table found')

        # associate new route table to vn
        net_req = {'vpc:route_table': route_table['fq_name']}
        neutron.update_network(net['id'], {'network': net_req})
        association_id = 'rtbassoc-' + net['id'][:8]
        return {'new_association_id': association_id}

    def delete_route_table(self, context, **kwargs):
        route_table_id = kwargs['route_table_id']
        neutron = neutronv2.get_client(context)

        # find the route table to delete
        try:
            route_rsp = neutron.list_route_tables()
            for route_table in route_rsp['route_tables']:
                if route_table['name'] == route_table_id:
                    # delete the specified route table
                    neutron.delete_route_table(route_table['id'])
                    break
        except Exception as e:
            raise exception.InvalidRequest(e)

        return {'return': True}

    def describe_route_tables(self, context, **kwargs):
        # fetch all route tables
        neutron = neutronv2.get_client(context)
        try:
            route_rsp = neutron.list_route_tables(tenant_id=context.project_id)
            net_rsp = neutron.list_networks(tenant_id=context.project_id)
        except Exception as e:
            raise exception.InvalidRequest(e)

        # formatting and filtering to display
        rt_passed = kwargs.get('route_table_id')
        route_tables = []
        for route_table in route_rsp['route_tables']:
            if rt_passed and rt_passed[0] != route_table['name']:
                continue

            route_tbl = {}
            route_tbl['routeTableId'] = route_table['name']
            route_tbl['vpc_id'] = route_table['fq_name'][1]

            route_tbl['routeSet'] = []
            for routes in route_table['routes'] or []:
                route = {}
                route['destination_cidr_block'] = routes['prefix']
                route['gatewayId'] = routes['next_hop']
                route['state'] = 'active'
                route['origin'] = 'CreateRouteTable'
                route_tbl['routeSet'].append(route)

            route_tbl['associationSet'] = []
            for net in net_rsp['networks']:
                if not 'vpc:route_table' in net:
                    continue
                domain, project, rt_name = net['vpc:route_table'][0]
                if rt_name != route_table['name']:
                    continue
                association = {}
                association['routeTableAssociationId'] = \
                    'rtbassoc-' + net['id'][0:8]
                association['routeTableId'] = rt_name
                if rt_name == 'rtb-default':
                    association['main'] = 'true'
                association['subnetId'] = net['name']
                route_tbl['associationSet'].append(association)

            route_tables.append(route_tbl)

        return {'routeTableSet': route_tables}

    def create_route(self, context, **kwargs):
        route_table_id = kwargs['route_table_id']
        cidr = kwargs['destination_cidr_block']
        if 'gateway_id' in kwargs:
            next_hop = kwargs['gateway_id']
            next_hop_type = None
        elif 'instance_id' in kwargs:
            instance_id = kwargs['instance_id']
            next_hop_type = instance_id
            uuid = ec2utils.ec2_inst_id_to_uuid(context, instance_id)
            instance = self.compute_api.get(context, uuid)
            if instance:
                next_hop = uuid
            else:
                raise exception.InvalidParameterValue(err="instance not found")
        elif 'interface_id' in kwargs:
            next_hop = kwargs['interface_id']
            next_hop_type = None
        else:
            raise exception.InvalidParameterValue(
                err="gateway/instance/interfacem id must be provided")
        neutron = neutronv2.get_client(context)

        try:
            # find the route table
            route_rsp = neutron.list_route_tables(tenant_id=context.project_id)
        except Exception as e:
            raise exception.InvalidRequest(e)

        foundRouteTable = False
        for route_table in route_rsp['route_tables']:
            if route_table['name'] == route_table_id:
                foundRouteTable = True
                break
        if not foundRouteTable:
            raise exception.InvalidParameterValue(
                err='No route table found')

        # find the route
        foundRoute = False
        for route in route_table['routes']:
            if route['prefix'] == cidr:
                route['next_hop'] = next_hop
                route['next_hop_type'] = next_hop_type
                foundRoute = True
                break

        if not foundRoute:
            route = {'prefix': cidr,
                     'next_hop': next_hop,
                     'next_hop_type': next_hop_type}
            route_table['routes'].append(route)

        # add route to the route table
        route_dict = {'route': route_table['routes']}
        req = {'routes': route_dict}
        try:
            route_rsp = neutron.update_route_table(route_table['id'],
                                                   {'route_table': req})
        except Exception as e:
            raise exception.InvalidRequest(e)

        return {'return': True}

    def replace_route(self, context, **kwargs):
        route_table_id = kwargs['route_table_id']
        cidr = kwargs['destination_cidr_block']
        if 'gateway_id' in kwargs:
            next_hop = kwargs['gateway_id']
            next_hop_type = None
        elif 'instance_id' in kwargs:
            instance_id = kwargs['instance_id']
            next_hop_type = instance_id
            uuid = ec2utils.ec2_inst_id_to_uuid(context, instance_id)
            instance = self.compute_api.get(context, uuid)
            if instance:
                next_hop = uuid
            else:
                raise exception.InvalidParameterValue(err="instance not found")
        elif 'interface_id' in kwargs:
            next_hop = kwargs['interface_id']
            next_hop_type = None
        else:
            raise exception.InvalidParameterValue(
                err="gateway/instance/interface id must be provided")
        neutron = neutronv2.get_client(context)

        try:
            # find the route table
            route_rsp = neutron.list_route_tables(tenant_id=context.project_id)
        except Exception as e:
            raise exception.InvalidRequest(e)

        foundRouteTable = False
        for route_table in route_rsp['route_tables']:
            if route_table['name'] == route_table_id:
                foundRouteTable = True
                break
        if not foundRouteTable:
            raise exception.InvalidParameterValue(
                err='No route table found')

        # find the route
        i = 0
        foundRoute = False
        for route in route_table['routes']:
            if route['prefix'] == cidr:
                foundRoute = True
                break
            i += 1
        if not foundRoute:
            raise exception.InvalidParameterValue(err='No route found')

        # replace route
        route_table['routes'][i]['next_hop'] = next_hop
        route_table['routes'][i]['next_hop_type'] = next_hop_type
        route_dict = {'route': route_table['routes']}
        req = {'routes': route_dict}
        try:
            route_rsp = neutron.update_route_table(route_table['id'],
                                                   {'route_table': req})
        except Exception as e:
            raise exception.InvalidRequest(e)

        return {'return': True}

    def delete_route(self, context, **kwargs):
        route_table_id = kwargs['route_table_id']
        cidr = kwargs['destination_cidr_block']
        neutron = neutronv2.get_client(context)

        try:
            # find the route table
            route_rsp = neutron.list_route_tables(tenant_id=context.project_id)
        except Exception as e:
            raise exception.InvalidRequest(e)

        foundRouteTable = False
        for route_table in route_rsp['route_tables']:
            if route_table['name'] == route_table_id:
                foundRouteTable = True
                break
        if not foundRouteTable:
            raise exception.InvalidParameterValue(
                err='No route table found')

        # find the route
        i = 0
        foundRoute = False
        for route in route_table['routes']:
            if route['prefix'] == cidr:
                foundRoute = True
                break
            i += 1
        if not foundRoute:
            raise exception.InvalidParameterValue(err='No route found')

        # delete route
        route_table['routes'].pop(i)
        route_dict = {'route': route_table['routes']}
        req = {'routes': route_dict}
        try:
            route_rsp = neutron.update_route_table(route_table['id'],
                                                   {'route_table': req})
        except Exception as e:
            raise exception.InvalidRequest(e)

        return {'return': True}

    def _populate_default_rule(self, direction, rule_idx, action):
        any_prefix = {'security_group': None, 
                      'subnet': {'ip_prefix': '0.0.0.0',
                                 'ip_prefix_len': 0},
                      'virtual_network': 'any'}
        local_prefix = {'security_group': None, 'subnet': None,
                        'virtual_network': 'local'}

        if direction == '<':
            rule_uuid = 'ingress-' + rule_idx
            src = any_prefix
            dst = local_prefix
        else:
            rule_uuid = 'egress-' + rule_idx
            src = local_prefix
            dst = any_prefix

        port_range = {'start_port': 0, 'end_port': 65535}
        rule = {'direction': '>', 'protocol': 'any',
                'dst_addresses': [dst], 'action_list': None,
                'rule_uuid': rule_uuid, 'dst_ports': [port_range],
                'application': [], 'action_list': {'simple_action': action},
                'rule_sequence': None, 'src_addresses': [src],
                'src_ports': [port_range]}

        return rule

    def create_network_acl(self, context, **kwargs):
        vpc_id = kwargs.get('vpc_id')[0]

        # get project id
        tenant_id = self._get_tenantid_from_vpcid(vpc_id, context)

        # create default rules
        pol_dict = {}
        pol_dict['policy_rule'] = []
        if 'default' in kwargs:
            rule = self._populate_default_rule('<', '100', 'pass')
            pol_dict['policy_rule'].append(rule)
            rule = self._populate_default_rule('>', '100', 'pass')
            pol_dict['policy_rule'].append(rule)

        rule = self._populate_default_rule('<', '32767', 'drop')
        pol_dict['policy_rule'].append(rule)
        rule = self._populate_default_rule('>', '32767', 'drop')
        pol_dict['policy_rule'].append(rule)

        # create network policy with default rules in current project
        neutron = neutronv2.get_client(context)
        try:
            if 'default' in kwargs:
                acl_id = 'acl-default'
            else:
                acl_id = 'acl-' + ('%x' % uuid.uuid4().time_low)
            policy_req = {'tenant_id': tenant_id, 'name': acl_id,
                          'entries': pol_dict}
            neutron.create_policy({'policy': policy_req})

        except Exception as e:
            raise exception.InvalidRequest(e)

        resp = {'networkAclId': acl_id, 'vpcId': vpc_id, 'default': 'false',
                'entrySet': [{'ruleNumber': '32767', 'protocol': 'all',
                              'ruleAction': 'deny', 'egress': 'true',
                              'cidrBlock': '0.0.0.0/0'},
                             {'ruleNumber': '32767', 'protocol': 'all',
                              'ruleAction': 'deny', 'egress': 'false',
                              'cidrBlock': '0.0.0.0/0'}]}
        return {'networkAcl': resp}

    def replace_network_acl_association(self, context, **kwargs):
        acl_id = kwargs.get('network_acl_id')
        association_id = kwargs.get('association_id')

        neutron = neutronv2.get_client(context)
        try:
            # find policy
            policys = neutron.list_policys(tenant_id=context.project_id)
        except Exception as e:
            raise exception.InvalidRequest(e)

        found_policy = False
        for pol in policys['policys']:
            if pol['name'] == acl_id:
                found_policy = True
                break

        try:
            # find network
            nets = neutron.list_networks(tenant_id=context.project_id)
        except Exception as e:
            raise exception.InvalidRequest(e)

        found_net = False
        for net in nets['networks']:
            if association_id == 'aclassoc-' + net['id'][:8]:
                found_net = True
                net_req = {'contrail:policys': [pol['fq_name']]}
                break

        if not (found_policy and found_net):
            raise exception.InvalidParameterValue(
                err='No network association')

        try:
            # update association
            neutron.update_network(net['id'], {'network': net_req})
        except Exception as e:
            raise exception.InvalidRequest(e)

        return {'new_association_id': association_id}

    def delete_network_acl(self, context, **kwargs):
        acl_id = kwargs.get('network_acl_id')

        # check if default acl
        if acl_id == 'acl-default':
            raise exception.InvalidParameterValue(
                err='Cannot delete default ACL')

        # find the acl from list and delete it
        # if associated with subnet throw exception
        neutron = neutronv2.get_client(context)
        try:
            policys = neutron.list_policys()
        except Exception as e:
            raise exception.InvalidRequest(e)

        for pol in policys['policys']:
            if pol['name'] == acl_id:
                break
        if 'nets_using' in pol:
            raise exception.InvalidParameterValue(
                err='Cannot delete network policy.Associeted with VN')

        try:
            neutron.delete_policy(pol['id'])
        except Exception as e:
            raise exception.InvalidRequest(e)

        return {'return': 'true'}

    def describe_network_acls(self, context, **kwargs):
        if 'acl_id' in kwargs:
            acl_id = kwargs.get('acl_id')
        acls = []

        neutron = neutronv2.get_client(context)
        try:
            policys = neutron.list_policys(tenant_id=context.project_id)
        except Exception as e:
            raise exception.InvalidRequest(e)

        for pol in policys['policys']:
            acl = {}
            if 'acl_id' in kwargs and pol['name'] != acl_id:
                continue
            if not pol['name'].startswith('acl-'):
                continue

            acl['vpc_id'] = pol['fq_name'][1]
            acl['default'] = 'false'
            acl['network_acl_id'] = pol['name']
            if pol['name'] == 'acl-default':
                acl['default'] = 'true'
            acl['entrySet'] = []

            if pol['entries'] and 'policy_rule' in pol['entries']:
                for rule in pol['entries']['policy_rule'] or []:
                    entry = {}
                    entry['ruleNumber'] = rule['rule_uuid'].split('-')[1]
                    if rule['protocol'] == 'any':
                        entry['protocol'] = '-1'
                    else:
                        entry['protocol'] = rule['protocol']

                    if rule['action_list']['simple_action'] == 'drop':
                        entry['ruleAction'] = 'deny'
                    else:
                        entry['ruleAction'] = 'allow'

                    if rule['rule_uuid'].startswith('egress-'):
                        entry['egress'] = 'true'
                        entry['portRange'] = {
                            'from': rule['dst_ports'][0]['start_port'],
                            'to': rule['dst_ports'][0]['end_port']}
                        cidr = rule['dst_addresses'][0]['subnet']
                        cidr_str = cidr['ip_prefix'] + '/' + \
                            str(cidr['ip_prefix_len'])
                        entry['cidrBlock'] = cidr_str
                    else:
                        entry['egress'] = 'false'
                        cidr = rule['src_addresses'][0]['subnet']
                        cidr_str = cidr['ip_prefix'] + '/' + \
                            str(cidr['ip_prefix_len'])
                        entry['cidrBlock'] = cidr_str
                        entry['portRange'] = {
                            'from': rule['src_ports'][0]['start_port'],
                            'to': rule['src_ports'][0]['end_port']}

                    acl['entrySet'].append(entry)

            if 'nets_using' in pol:
                try:
                    nets = neutron.list_networks(tenant_id=context.project_id)
                except Exception as e:
                    raise exception.InvalidRequest(e)

                associated_net = [net[2] for net in pol['nets_using']]
                acl['associationSet'] = []
                for net in nets['networks']:
                    assoc = {}
                    if net['name'] in associated_net:
                        assoc['networkAclAssociationId'] = \
                            'aclassoc-' + net['id'][:8]
                        assoc['networkAclId'] = pol['name']
                        assoc['subnetId'] = net['name']
                        acl['associationSet'].append(assoc)

            acls.append(acl)

        return {'networkAclSet': acls}

    def create_network_acl_entry(self, context, **kwargs):
        # create rule
        acl_id = kwargs.get('network_acl_id')
        pol_dict = self._create_policy_rule(context, kwargs)
        rule_no = pol_dict['policy_rule'][0]['rule_uuid']

        # add new policy entry to list and update acl policy
        neutron = neutronv2.get_client(context)
        try:
            policys = neutron.list_policys(tenant_id=context.project_id)
            for pol in policys['policys']:
                if pol['name'] == acl_id:
                    break

            pol_list = pol['entries']['policy_rule']
            rule_id_list = [rule['rule_uuid'] for rule in pol_list]

            # check if rule number already exsists
            if rule_no in rule_id_list:
                pol_list[rule_id_list.index(rule_no)] = \
                    pol_dict['policy_rule'][0]
            else:
                pol_list.append(pol_dict['policy_rule'][0])
                pol_list = sorted(pol_list, key=lambda k: int(
                    k['rule_uuid'].split('-')[1]))

            pol_list_dict = {'policy_rule': pol_list}
            policy_req = {'entries': pol_list_dict}
            neutron.update_policy(pol['id'], {'policy': policy_req})

        except Exception as e:
            raise exception.InvalidRequest(e)

        return {'return': 'true'}

    def replace_network_acl_entry(self, context, **kwargs):
        # create rule
        acl_id = kwargs.get('network_acl_id')
        pol_dict = self._create_policy_rule(context, kwargs)
        rule_no = pol_dict['policy_rule'][0]['rule_uuid']

        # replace entry in acl policy list and update acl
        neutron = neutronv2.get_client(context)
        try:
            policys = neutron.list_policys(tenant_id=context.project_id)
        except Exception as e:
            raise exception.InvalidRequest(e)

        for pol in policys['policys']:
            if pol['name'] == acl_id:
                break
        i = 0
        found = False
        pol_list = pol['entries']['policy_rule']
        for rule in pol_list:
            if rule['rule_uuid'] == rule_no:
                found = True
                break
            i += 1

        if found:
            pol_list[i] = pol_dict['policy_rule'][0]
            pol_list_dict = {'policy_rule': pol_list}
        else:
            raise exception.InvalidParameterValue(
                err='No matching ACL entry found')

        policy_req = {'entries': pol_list_dict}
        try:
            neutron.update_policy(pol['id'], {'policy': policy_req})
        except Exception as e:
            raise exception.InvalidRequest(e)

        return {'return': 'true'}

    def delete_network_acl_entry(self, context, **kwargs):
        acl_id = kwargs.get('network_acl_id')
        rule_no = kwargs.get('rule_number')

        direction = 'ingress'
        if 'egress' in kwargs and kwargs['egress']:
            direction = 'egress'

        if direction == 'egress':
            rule_no = 'egress-' + str(kwargs.get('rule_number'))
            direction = '>'
        elif direction == 'ingress':
            rule_no = 'ingress-' + str(kwargs.get('rule_number'))
            direction = '<'
        else:
            raise exception.InvalidParameterValue(
                err='Invalid direction argument')

        # delete policy entry and update acl
        neutron = neutronv2.get_client(context)
        try:
            policys = neutron.list_policys(tenant_id=context.project_id)
        except Exception as e:
            raise exception.InvalidRequest(e)

        for pol in policys['policys']:
            if pol['name'] == acl_id:
                break
        i = 0
        found = False
        pol_list = pol['entries']['policy_rule']
        for rule in pol_list:
            if rule['rule_uuid'] == rule_no:
                found = True
                break
            i += 1
        if found:
            pol_list.pop(i)
            pol_list_dict = {'policy_rule': pol_list}
        else:
            raise exception.InvalidParameterValue(
                err='No matching ACL entry found')

        policy_req = {'entries': pol_list_dict}
        try:
            neutron.update_policy(pol['id'], {'policy': policy_req})
        except Exception as e:
            raise exception.InvalidRequest(e)

        return {'return': 'true'}

    def vpc_create_security_group(self, context, group_name,
                                  group_description, vpc_id=None):
        # get project id
        tenant_id = self._get_tenantid_from_vpcid(vpc_id, context)
        neutron = neutronv2.get_client(context)

        # set security group
        group_req = {'description': group_description,
                     'name': group_name,
                     'tenant_id': tenant_id}
        try:
            group_rsp = neutron.create_security_group(
                {'security_group': group_req})
            group_ref = group_rsp['security_group']
            sg_id = "sg-" + group_ref['id'][0:8]
            return {'return': 'true', 'groupId': sg_id}
        except Exception as e:
            raise exception.InvalidRequest(e)

    def vpc_delete_security_group(self, context, group_name=None,
                                  group_id=None, kwargs=None):
        neutron = neutronv2.get_client(context)
        groups = neutron.list_security_groups()
        for group in groups['security_groups']:
            sg_id = "sg-" + group['id'][0:8]
            if sg_id == group_id:
                neutron.delete_security_group(group['id'])

        return {'return': 'true'}

    def authorize_security_group_egress(self, context, group_name=None,
                                        group_id=None, **kwargs):
        req = self._get_security_group_rule_params(context, kwargs)
        req['direction'] = 'egress'
        req['security_group_id'] = \
            self._get_group_uuid_from_group_id(context, group_id)

        # check if rule with specified parameters already exists
        if not self._get_rule_uuid_from_params(context, req):
            try:
                # create rule in given security group
                neutron = neutronv2.get_client(context)
                neutron.create_security_group_rule(
                    {'security_group_rule': req})
                return {'return': 'true'}
            except Exception as e:
                raise exception.InvalidRequest(e)
        else:
            raise exception.SecurityGroupRuleExists(
                message='Rule for the specified parameters already exists.')

    def vpc_authorize_security_group_ingress(self, context, group_name=None,
                                             group_id=None, kwargs=None):
        # set the rules from function arguments
        req = self._get_security_group_rule_params(context, kwargs)
        req['direction'] = 'ingress'
        req['security_group_id'] = \
            self._get_group_uuid_from_group_id(context, group_id)

        # check if rule with specified parameters already exists
        if not self._get_rule_uuid_from_params(context, req):
            try:
                # create rule in given security group
                neutron = neutronv2.get_client(context)
                neutron.create_security_group_rule(
                    {'security_group_rule': req})
                return {'return': 'true'}
            except Exception as e:
                raise exception.InvalidRequest(e)
        else:
            raise exception.SecurityGroupRuleExists(
                message='Rule for the specified parameters already exists.')

    def vpc_revoke_security_group_ingress(self, context, group_name=None,
                                          group_id=None, kwargs=None):
        # set the rule parameters from function arguments
        req = self._get_security_group_rule_params(context, kwargs)
        req['direction'] = 'ingress'
        req['security_group_id'] = \
            self._get_group_uuid_from_group_id(context, group_id)

        # check if rule with specified parameter exists or not
        rule_id = self._get_rule_uuid_from_params(context, req)
        if rule_id:
            try:
                # delete the rule
                neutron = neutronv2.get_client(context)
                neutron.delete_security_group_rule(rule_id)
                return {'return': 'true'}
            except Exception as e:
                raise exception.InvalidRequest(e)
        else:
            raise exception.InvalidParameterValue(
                message='No rule for the specified parameters.')

    def revoke_security_group_egress(self, context, group_name=None,
                                     group_id=None, **kwargs):
        # set the rule parameters from function arguments
        req = self._get_security_group_rule_params(context, kwargs)
        req['direction'] = 'egress'
        req['security_group_id'] = \
            self._get_group_uuid_from_group_id(context, group_id)

        # check if rule with specified parameter exists or not
        rule_id = self._get_rule_uuid_from_params(context, req)
        if rule_id:
            try:
                # delete the rule
                neutron = neutronv2.get_client(context)
                neutron.delete_security_group_rule(rule_id)
                return {'return': 'true'}
            except Exception as e:
                raise exception.InvalidRequest(e)
        else:
            raise exception.InvalidParameterValue(
                message='No rule for the specified parameters.')

    def vpc_describe_security_groups(self, context, search_opts, 
                                     group_name=None, group_id=None):
        neutron = neutronv2.get_client(context)
        tenant_id = self._get_tenantid_from_vpcid(search_opts['vpc_id'],
                                                  context)

        try:
            groups = neutron.list_security_groups(tenant_id=tenant_id)
        except Exception as e:
            raise exception.InvalidRequest(e)

        grps = []
        for group in groups['security_groups']:
            sg_id = "sg-" + group['id'][0:8]
            grp = {}

            if group_id and sg_id not in group_id:
                continue
            if group_name and group['name'] not in group_name:
                continue

            kc = self._get_keystone_client(context)
            try:
                tenant = kc.tenants.get(group['tenant_id'])
                grp['vpcId'] = tenant.name
            except Exception as e:
                raise exception.InvalidRequest('Keystone exception %s' % e)

            grp['groupId'] = sg_id
            grp['groupName'] = group['name']
            grp['groupDescription'] = group['description']

            ingress_rules = []
            egress_rules = []
            for rule in group['security_group_rules']:
                sg_rule = {}
                sg_rule['ip_protocol'] = rule['protocol']
                sg_rule['from_port'] = rule['port_range_min']
                sg_rule['to_port'] = rule['port_range_max']
                if rule['remote_ip_prefix']:
                    cidr = {'cidr_ip': rule['remote_ip_prefix']}
                    sg_rule['ip_ranges'] = [cidr]
                    sg_rule['groups'] = []
                elif rule['remote_group_id']:
                    sg_id = "sg-" + rule['remote_group_id'][0:8]
                    grp_id = {'group_id': sg_id}
                    sg_rule['groups'] = [grp_id]
                    sg_rule['ip_ranges'] = []

                if rule['direction'] == 'ingress':
                    ingress_rules.append(sg_rule)
                else:
                    egress_rules.append(sg_rule)

            grp['ipPermissions'] = ingress_rules
            grp['ipPermissionsEgress'] = egress_rules
            grps.append(grp)

        return {'security_group_info': grps}

    def vpc_format_address(self, context, floating_ip, neutron):
        ec2_id = None
        assoc_id = None
        address = {}

        if 'port_id' in floating_ip and floating_ip['port_id']:
            port = neutron.show_port(floating_ip['port_id'])
            inst_id = port['port']['device_id']
            ec2_id = ec2utils.id_to_ec2_inst_id(inst_id)
            assoc_id = 'eipassoc-' + floating_ip['id'][:8]

        address = {'public_ip': floating_ip['floating_ip_address'],
                   'instance_id': ec2_id,
                   'association_id': assoc_id,
                   'private_ip_address': None}

        address['domain'] = 'vpc'
        address['allocation_id'] = 'eipalloc-' + floating_ip['id'][:8]

        return address

    def vpc_allocate_address(self, context, kwargs):
        vpc_id = self._get_vpcid_from_context(context)
        neutron = neutronv2.get_client(context)
        try:
            nw_list = neutron.list_networks()
        except Exception as e:
            raise exception.InvalidRequest(e)

        # find floating pool VN
        fip = None
        for nw in nw_list['networks']:
            if nw['name'] != 'public':
                continue

            if ('admin' != self._get_vpcid_from_tenantid(nw['tenant_id'],
                                                         context)):
                continue

            # allocate floating ip
            try:
                fip_req = {'floatingip':
                          {'floating_network_id': nw['id'],
                           'tenant_id': context.project_id}}
                fip_resp = neutron.create_floatingip(fip_req)
                fip = fip_resp['floatingip']['floating_ip_address']
                eip_id = 'eipalloc-' + fip_resp['floatingip']['id'][:8]
            except Exception as e:
                raise exception.InvalidRequest(message='public network not provisioned')

        if fip:
            return {'publicIp': fip, 'domain': 'vpc', 'allocationId': eip_id}
        else:
            raise exception.InvalidRequest(message='public network not provisioned')

    def vpc_release_address(self, context, public_ip=None, kwargs=None):
        eip_id = kwargs.get('allocation_id').split('-')[1]
        LOG.audit(_("Release address %s"), eip_id, context=context)

        # return floating ip to the floating ip pool
        neutron = neutronv2.get_client(context)
        try:
            fip_list = neutron.list_floatingips(
                tenant_id=context.project_id)
            for fip in fip_list['floatingips']:
                if eip_id:
                    if fip['id'][:8] != eip_id:
                        continue
                elif public_ip:
                    if fip['floating_ip_address'] != public_ip:
                        continue

                neutron.delete_floatingip(fip['id'])
                return {'return': "true"}
        except Exception as e:
            raise exception.InvalidRequest(e)

    def vpc_associate_address(self, context, instance_id,
                              public_ip=None, kwargs=None):
        eip_id = kwargs.get('allocation_id').split('-')[1]
        instance_uuid = kwargs['instance_uuid']
        LOG.audit(_("Associate address %(eip_id)s to instance "
                    "%(instance_id)s"),
                  {'eip_id': eip_id, 'instance_id': instance_id},
                  context=context)
        neutron = neutronv2.get_client(context)
        fip_list = neutron.list_floatingips(tenant_id=context.project_id)
        found = False
        for fip in fip_list['floatingips']:
            if eip_id:
                if fip['id'][:8] == eip_id:
                    found = True
                    break
            elif public_ip:
                if fip['floating_ip_address'] == public_ip:
                    found = True
                    break

        if not found:
            raise exception.InvalidParameterValue(err="Value not found")

        try:
            ports = neutron.list_ports(device_id=[instance_uuid])
            for port in ports['ports']:
                fip_req = {'floatingip': {'port_id': port['id']}}
                neutron.update_floatingip(fip['id'], fip_req)
                association_id = 'eipassoc-' + fip['id'][:8]
                return {'return': 'true',
                        'association_id': association_id}
        except Exception as e:
            raise exception.InvalidRequest(e)

    def vpc_disassociate_address(self, context, public_ip=None, kwargs=None):
        eip_id = kwargs.get('association_id').split('-')[1]
        LOG.audit(_("Disassociate address %s"), public_ip, context=context)

        # fecth the matching floating ip
        neutron = neutronv2.get_client(context)
        fip_list = neutron.list_floatingips(tenant_id=context.project_id)
        found = False
        for fip in fip_list['floatingips']:
            if eip_id:
                if fip['id'][:8] == eip_id:
                    found = True
                    break
            elif public_ip:
                if fip['floating_ip_address'] == public_ip:
                    found = True
                    break

        if not found:
            raise exception.InvalidParameterValue(err="Given value not found")

        # disassociate the floating ip
        try:
            fip_req = {'floatingip': {'port_id': None}}
            neutron.update_floatingip(fip['id'], fip_req)
        except Exception as e:
            raise exception.InvalidRequest(e)

        return {'return': "true"}

    def vpc_describe_addresses(self, context, kwargs):
        neutron = neutronv2.get_client(context)
        floatings = neutron.list_floatingips(tenant_id=context.project_id)
        addresses = [self.vpc_format_address(
            context, f, neutron) for f in floatings['floatingips']]
        return {'addressesSet': addresses}

    def create_internet_gateway(self, context):
        return {'internetGateway': {'internetGatewayId': 'igw-default'}}

    def delete_internet_gateway(self, context, **kwargs):
        return {'return': "true"}

    def attach_internet_gateway(self, context, **kwargs):
        vpc_id = kwargs['vpc_id']
        tenant_id = self._get_tenantid_from_vpcid(vpc_id, context)
        igw = kwargs['internet_gateway_id']
        if igw != 'igw-default':
            raise exception.InvalidRequest('Gateway should be igw-default')
        return {'return': "true"}

    def detach_internet_gateway(self, context, **kwargs):
        vpc_id = kwargs['vpc_id']
        tenant_id = self._get_tenantid_from_vpcid(vpc_id, context)
        igw = kwargs['internet_gateway_id']
        if igw != 'igw-default':
            raise exception.InvalidRequest('Gateway should be igw-default')
        return {'return': "true"}

    def describe_internet_gateways(self, context):
        return {'internetGatewaySet': [{'internetGatewayId': 'igw-default'}]}

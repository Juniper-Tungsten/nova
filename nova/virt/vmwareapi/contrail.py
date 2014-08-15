# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright (c) 2013 Hewlett-Packard Development Company, L.P.
# Copyright (c) 2012 VMware, Inc.
# Copyright (c) 2011 Citrix Systems, Inc.
# Copyright 2011 OpenStack Foundation
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
Contrail wrapper around ESX
"""

import re
import time
import socket
import sys
import uuid

from oslo.config import cfg
from nova import exception
from nova.openstack.common import log as logging
from nova.openstack.common import loopingcall
from nova.openstack.common import uuidutils
from nova.virt import driver
from nova.virt.vmwareapi import error_util
from nova.virt.vmwareapi import host
from nova.virt.vmwareapi import vim
from nova.virt.vmwareapi import vim_util
from nova.virt.vmwareapi import vm_util
from nova.virt.vmwareapi import vmops
from nova.virt.vmwareapi import volumeops
from nova.virt.vmwareapi import network_util

from nova.virt.vmwareapi.driver import VMwareESXDriver
from bitstring import BitArray
from thrift.transport import TTransport, TSocket
from thrift.transport.TTransport import TTransportException
from thrift.protocol import TBinaryProtocol, TProtocol
from nova_contrail_vif.gen_py.instance_service import InstanceService
from nova_contrail_vif.gen_py.instance_service import ttypes

LOG = logging.getLogger(__name__)

vmwareapi_contrail_opts = [
    cfg.StrOpt('vmpg_vswitch',
               help='Vswitch name to use to instantiate VMs incase of Contaril ESX solution'),
    ]
CONF = cfg.CONF
CONF.register_opts(vmwareapi_contrail_opts, 'vmware')

class ContrailVIFDriver(object):
    """to inform agent"""
    def __init__(self):
        super(ContrailVIFDriver, self).__init__()
        self._agent_alive = False
        self._agent_connected = False
        self._port_dict = {}
        self._protocol = None
        timer = loopingcall.FixedIntervalLoopingCall(self._keep_alive)
        timer.start(interval=2)
    #end __init__

    def _agent_connect(self, protocol):
        # Agent connect for first time
        #if self._agent_connected == False and protocol != None:
        if protocol != None:
            service = InstanceService.Client(protocol)
            return service.Connect()
        else:
            return False
    #end __agent_connect

    def _keep_alive(self):
        try:
            if self._agent_alive == False:
                self._protocol = self._agent_conn_open()
                if self._protocol == None:
                    return

            service = InstanceService.Client(self._protocol)
            aa_latest = service.KeepAliveCheck()
            if self._agent_alive == False and aa_latest == True:
                port_l = [v for k, v in self._port_dict.iteritems()]
                LOG.debug(('Agent sending port list %d, %s'), len(port_l), self)
                if len(port_l):
                    for i in range(len(port_l)):
                        LOG.debug(('Port %s %s'), port_l[i].tap_name, port_l[i].ip_address)

                service.AddPort(port_l)

                self._agent_alive = True

            if self._agent_alive == True and aa_latest == False:
                LOG.debug(('Agent not available, %s'), self)
                self._agent_alive = False
                return

        except:
            self._agent_alive = False
            LOG.debug(('Agent keep alive exception: %s'), self)
    #end _keep_alive

    def _agent_conn_open(self):
        try:
            socket = TSocket.TSocket("127.0.0.1", 9090)
            transport = TTransport.TFramedTransport(socket)
            transport.open()
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            self._agent_connected = self._agent_connect(protocol)
            return protocol
        except TTransportException:
            return None
    #end _agent_conn_open

    def _convert_to_bl(self, id):
        hexstr = uuid.UUID(id).hex
        return [int(hexstr[i:i+2], 16) for i in range(32) if i%2 == 0]
    #end _convert_to_bl

    def _agent_inform(self, port, id, add):
        # First add to the port list
        if add == True:
            self._port_dict[id] = port
        else:
            if id in self._port_dict:
                del self._port_dict[id]

        if not self._agent_alive:
            return

        LOG.debug(('agent_inform %s, %s, %s, %s'),
                  port.ip_address,
                  port.tap_name,
                  add,
                  self)
        try:
            service = InstanceService.Client(self._protocol)
            if add == True:
                service.AddPort([port])
            else:
                service.DeletePort(port.port_id)
        except:
            self._agent_alive = False

    #end _agent_inform

    def get_config(self, instance, network, mapping, image_meta):
        conf = super(VRouterVIFDriver, self).get_config(instance, network, mapping, image_meta)
        dev = self.get_vif_devname(mapping)
        designer.set_vif_host_backend_ethernet_config(conf, dev)

        return conf

    def plug(self, instance, vif, vlan_id):
        ip = 0
        network = vif['network']
        subnets_v4 = [s for s in network['subnets'] if s['version'] == 4]
        if len(subnets_v4[0]['ips']) > 0:
            ip = subnets_v4[0]['ips'][0]['address']

        # port_id(tuuid), instance_id(tuuid), tap_name(string), 
        # ip_address(string), vn_id(tuuid)
        port = ttypes.Port(self._convert_to_bl(vif['id']),
                           self._convert_to_bl(instance['uuid']),
                           network['bridge'],
                           ip,
                           self._convert_to_bl(vif['network']['id']),
                           vif['address'],
                           instance['display_name'],
                           instance['hostname'],
                           instance['host'],
                           self._convert_to_bl(instance['project_id']),
                           vlan_id)
        self._agent_inform(port, vif['id'], True)
    #end plug

    def unplug(self, instance, vif):
        """Unplug the VIF from the network by deleting the port from
        the bridge."""
        ip = 0
        if not vif:
            return

        network = vif['network']
        subnets_v4 = [s for s in network['subnets'] if s['version'] == 4]
        if len(subnets_v4[0]['ips']) > 0:
            ip = subnets_v4[0]['ips'][0]['address']

        port = ttypes.Port(self._convert_to_bl(vif['id']),
                           self._convert_to_bl(instance['uuid']),
                           network['bridge'],
                           ip,
                           self._convert_to_bl(vif['network']['id']),
                           vif['address'],
                           instance['display_name'],
                           instance['hostname'],
                           instance['host'])
        self._agent_inform(port, vif['id'], False)



INVALID_VLAN_ID = 4096
MAX_VLAN_ID = 4095

class ESXVlans(object):
    """The vlan object"""

    def __init__(self, esx_session):
        self._vlan_id_bits = BitArray(MAX_VLAN_ID + 1)
        self._init_vlans(esx_session)
    #end __init__()

    def _init_vlans(self, esx_session):
        session = esx_session
        host_mor = vm_util.get_host_ref(session)
        port_grps_on_host_ret = session._call_method(vim_util,
                                                     "get_dynamic_property",
                                                     host_mor,
                                                     "HostSystem",
                                                     "config.network.portgroup")
        if not port_grps_on_host_ret:
            return

        port_grps_on_host = port_grps_on_host_ret.HostPortGroup
        for p_gp in port_grps_on_host:
            if p_gp.spec.vlanId >= 0:
                self._vlan_id_bits[p_gp.spec.vlanId] = 1
    #end _init_vlans()

    def alloc_vlan(self):
        vid = self._vlan_id_bits.find('0b0')
        if vid:
            self._vlan_id_bits[vid[0]] = 1
            return vid[0]
        return INVALID_VLAN_ID
    #end alloc_vlan()

    def free_vlan(self, vlan_id):
        if vlan_id < 0 or vlan_id > MAX_VLAN_ID:
            return
        self._vlan_id_bits[vlan_id] = 0
    #end free_vlan()

#end ESXVlans


        
class ContrailESXDriver(VMwareESXDriver):
    """Sub class of ESX"""

    def __init__(self, virtapi, read_only=False, scheme="https"):
        super(ContrailESXDriver, self).__init__(virtapi)
        self.VifInfo = ContrailVIFDriver()
        self.Vlan = ESXVlans(self._session)

    def remove_port_group(self, name):
        session = self._session
        host_mor = vm_util.get_host_ref(session)
        network_system_mor = session._call_method(vim_util,
                                                  "get_dynamic_property",
                                                  host_mor,
                                                  "HostSystem",
                                                  "configManager.networkSystem")
        try:
            session._call_method(session._get_vim(),
                                 "RemovePortGroup", network_system_mor,
                                 pgName=name) 
        except error_util.VimFaultException as exc:
            pass
        

    def spawn(self, context, instance, image_meta, injected_files,
              admin_password, network_info=None, block_device_info=None):
        session = self._session
        if network_info:
            for vif in network_info:
                vlan_id = self.Vlan.alloc_vlan()
                if vlan_id == INVALID_VLAN_ID:
                    raise exception.NovaException("Vlan id space is full")

                vif['network']['bridge'] = vif['network']['label'] + \
                                            '-' + str(instance['hostname']) + \
                                            '-' + str(vif['id'])
                args = {'should_create_vlan':True, 'vlan':vlan_id}
                vif['network']._set_meta(args)
                network_util.create_port_group(session,
                                                vif['network']['bridge'],
                                                CONF.vmware.vmpg_vswitch,
                                                vlan_id)
                self.VifInfo.plug(instance, vif, vlan_id)

        super(ContrailESXDriver, self).spawn(context, instance, image_meta,
                                             injected_files, admin_password,
                                             network_info, block_device_info)

    def destroy(self, instance, network_info, block_device_info=None,
                destroy_disks=True, context=None):
        session = self._session
        vlan_id = None
        vswitch = ""

        if network_info:
            for vif in network_info:
                self.VifInfo.unplug(instance, vif)

        super(ContrailESXDriver, self).destroy(instance, network_info,
                block_device_info, destroy_disks, context)

        if not network_info:
            return

        for vif in network_info:
            port_group = vif['network']['label'] + \
                          '-' + str(instance['hostname']) + \
                          '-' + str(vif['id'])

            try:
                vlan_id, vswitch = \
                    network_util.get_vlanid_and_vswitch_for_portgroup(session,
                                                                      port_group)
            except TypeError:
                pass
            if not vlan_id:
                return

            self.remove_port_group(port_group)
            self.Vlan.free_vlan(vlan_id)

    def plug_vifs(self, instance, network_info):
        """Plug VIFs into networks."""

        session = self._session
        for vif in network_info:
            vlan_id = None
            vswitch = ""
            port_group = vif['network']['label'] + \
                          '-' + str(instance['hostname']) + \
                          '-' + str(vif['id'])
            try:
                vlan_id, vswitch = \
                    network_util.get_vlanid_and_vswitch_for_portgroup(session,
                                                                      port_group)
            except TypeError:
                pass
            if not vlan_id:
                continue
            vif['network']['bridge'] = vif['network']['label'] + \
                                            '-' + str(instance['hostname']) + \
                                            '-' + str(vif['id'])
            self.VifInfo.plug(instance, vif, vlan_id)

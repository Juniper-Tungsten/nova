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
from contrail_vrouter_api.vrouter_api import ContrailVRouterApi
from thrift.Thrift import TApplicationException
from nova.openstack.common import processutils
from nova.virt.libvirt import designer

LOG = logging.getLogger(__name__)

vmwareapi_contrail_opts = [
    cfg.StrOpt('vmpg_vswitch',
               help='Vswitch name to use to instantiate VMs incase of Contaril ESX solution'),
    ]
CONF = cfg.CONF
CONF.register_opts(vmwareapi_contrail_opts, 'vmware')

class ContrailVIFDriver(object):
    """to inform agent"""

    PORT_TYPE = 'NovaVMPort'

    def __init__(self):
        super(ContrailVIFDriver, self).__init__()
        self._vrouter_client = ContrailVRouterApi(doconnect=True)
        timer = loopingcall.FixedIntervalLoopingCall(self._keep_alive)
        timer.start(interval=2)
    #end __init__

    def _keep_alive(self):
        self._vrouter_client.periodic_connection_check()

    def get_config(self, instance, network, mapping, image_meta):
        conf = super(VRouterVIFDriver, self).get_config(instance, network, mapping, image_meta)
        dev = self.get_vif_devname(mapping)
        designer.set_vif_host_backend_ethernet_config(conf, dev)

        return conf

    def plug(self, instance, vif, vlan_id):
        ipv4_address = '0.0.0.0'
        ipv6_address = None
        subnets = vif['network']['subnets']
        for subnet in subnets:
            ips = subnet['ips'][0]
            if (ips['version'] == 4):
                if ips['address'] is not None:
                    ipv4_address = ips['address']
            if (ips['version'] == 6):
                if ips['address'] is not None:
                    ipv6_address = ips['address']

        network = vif['network']

        kwargs = {
            'ip_address': ipv4_address,
            'vn_id': vif['network']['id'],
            'display_name': instance['display_name'],
            'hostname': instance['hostname'],
            'host': instance['host'],
            'vm_project_id': instance['project_id'],
            'port_type': self.PORT_TYPE,
            'ip6_address': ipv6_address,
            'vlan' : vlan_id,
        }
        try:
            result = self._vrouter_client.add_port(instance['uuid'],
                                                   vif['id'],
                                                   network['bridge'],
                                                   vif['address'],
                                                   **kwargs)
            if not result:
                LOG.exception(_LE("Failed while plugging vif"),
                              instance=instance)
        except TApplicationException:
            LOG.exception(_LE("Failed while plugging vif"), instance=instance)

    #end plug

    def unplug(self, instance, vif):
        """Unplug the VIF from the network by deleting the port from
        the bridge."""
        if not vif:
            return
        try:
            self._vrouter_client.delete_port(vif['id'])
        except (TApplicationException, processutils.ProcessExecutionError,\
        RuntimeError):
            LOG.exception(_LE("Failed while unplugging vif"),
                          instance=instance)


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
        self._vm_info = { }

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

    def get_portgroup_name(self, vif, instance):
        return vif['network']['label'] + '-' + str(instance['hostname']) \
            + '-' + str(vif['id'])

    def spawn(self, context, instance, image_meta, injected_files,
              admin_password, network_info=None, block_device_info=None):
        session = self._session

        vm_uuid = instance['uuid']

        vm_info = self._vm_info.get(vm_uuid)
        if vm_info is None:
            self._vm_info[vm_uuid] = { }

        if network_info:
            for vif in network_info:
                vif_uuid = vif['id']
                if self._vm_info[vm_uuid].get(vif_uuid) is None:
                    self._vm_info[vm_uuid] [vif_uuid] = { }
                    vlan_id = self.Vlan.alloc_vlan()
                    if vlan_id == INVALID_VLAN_ID:
                        raise exception.NovaException("Vlan id space is full")
                    portgroup = self.get_portgroup_name(vif, instance)
                    vif['network'] ['bridge'] = portgroup

                    # Store portgroup and vlan-id for VIF
                    self._vm_info[vm_uuid][vif_uuid]['vlan_id'] = vlan_id
                    self._vm_info[vm_uuid][vif_uuid]['portgroup'] = portgroup
                else:
                    portgroup = self._vm_info[vm_uuid][vif_uuid]['portgroup']
                    vlan_id = self._vm_info[vm_uuid][vif_uuid]['vlan_id']
                self._vm_info[vm_uuid][vif_uuid]['vif'] = vif

                args = {'should_create_vlan':True, 'vlan':vlan_id}
                vif['network']._set_meta(args)
                network_util.create_port_group(session, portgroup,
                                               CONF.vmware.vmpg_vswitch,
                                               vlan_id)
                self.VifInfo.plug(instance, vif, vlan_id)

        try:
            super(ContrailESXDriver, self).spawn(context, instance, image_meta,
                                                 injected_files, admin_password,
                                                 network_info, block_device_info)
        except Exception as exc:
            LOG.exception(exc, instance=instance)
            pass
        return

    def destroy(self, context, instance, network_info, block_device_info=None,
                destroy_disks=True):
        session = self._session
        vm_uuid = instance['uuid']
        vm_info = self._vm_info.get(vm_uuid)
        if not vm_info:
            return

        # In case of exception during spawn, the network_info may be reset.
        # Do unplug of VIF and portgroup/vlan delete based on values
        # from _vm_info
        try:
            for vif in vm_info:
                vif_info = self._vm_info[vm_uuid][vif]
                self.VifInfo.unplug(instance, vif_info['vif'])
        except Exception as exc:
            LOG.exception(_LE("Error in vif unplug"),
                          instance=instance)

        # Remove the VM
        try:
            super(ContrailESXDriver, self).destroy(context, instance, network_info,
                  block_device_info, destroy_disks)
        except Exception as exc:
            LOG.exception(exc, instance=instance)

        # Remove all port-groups and free the VLAN allocated for the vif
        try:
            for vif in vm_info:
                vif_info = self._vm_info[vm_uuid][vif]
                self.remove_port_group(vif_info['portgroup'])
                self.Vlan.free_vlan(vif_info['vlan_id'])
        except Exception as exc:
            LOG.exception(_LE("Error removing portgroup / VLAN for vif"),
                          instance=instance)
        del self._vm_info[vm_uuid]

    def plug_vifs(self, instance, network_info):
        """Plug VIFs into networks."""

        session = self._session
        vm_uuid = instance['uuid']
        vm_info = self._vm_info.get(vm_uuid)
        if vm_info is None:
            self._vm_info[vm_uuid] = { }

        # On nova-compute restart, vm_info is not populated.
        # However, network_info is populated in all cases. So, pick
        # configuration from network_info
        for vif in network_info:
            vlan_id = None
            vswitch = ''
            vif_uuid = vif['id']

            vif_info = self._vm_info[vm_uuid].get(vif_uuid)
            if vif_info is None:
                # vm_info not found. Most likely a case on nova-compute restart
                # In case of restart, plug_vifs is called without spawn
                # Read the port-group, vlan and populate vm_info
                portgroup = self.get_portgroup_name(vif, instance)
                try:
                    vlan_id, vswitch = \
                        network_util.get_vlanid_and_vswitch_for_portgroup(session,
                                                                      portgroup)
                except TypeError:
                    pass
                if not vlan_id:
                    continue

                vif['network']['bridge'] = portgroup
                # Copy vlan_id, portgroup and vif into vm_info
                self._vm_info[vm_uuid][vif_uuid] = { }
                self._vm_info[vm_uuid][vif_uuid]['vlan_id'] = vlan_id
                self._vm_info[vm_uuid][vif_uuid]['portgroup'] = portgroup

            self._vm_info[vm_uuid][vif_uuid]['vif'] = vif
            # Plug the VIF
            self.VifInfo.plug(instance, vif, vlan_id)

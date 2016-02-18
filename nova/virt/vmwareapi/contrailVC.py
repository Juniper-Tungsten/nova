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
Contrail wrapper around VCenter
"""

import re
import time
import socket
import sys
import uuid

from oslo.config import cfg
from oslo.vmware import exceptions as vexc
from oslo_log import log as logging
from oslo_utils import uuidutils
from nova import exception
from nova.openstack.common import loopingcall
from nova.virt import driver
from nova.virt.vmwareapi import error_util
from nova.virt.vmwareapi import host
from nova.virt.vmwareapi import vim_util
from nova.virt.vmwareapi import vm_util
from nova.virt.vmwareapi import vmops
from nova.virt.vmwareapi import volumeops
from nova.virt.vmwareapi import network_util

from nova.virt.vmwareapi.driver import VMwareVCDriver
from bitstring import BitArray
#from thrift.transport import TTransport, TSocket
#from thrift.transport.TTransport import TTransportException
#from thrift.protocol import TBinaryProtocol, TProtocol
#from nova_contrail_vif.gen_py.instance_service import InstanceService
#from nova_contrail_vif.gen_py.instance_service import ttypes

LOG = logging.getLogger(__name__)

vmwareapi_contrail_opts = [
    cfg.StrOpt('vcenter_dvswitch',
               help='Vswitch name to use to instantiate VMs incase of Contaril VCenter solution'),
    ]
CONF = cfg.CONF
CONF.register_opts(vmwareapi_contrail_opts, 'vmware')

INVALID_VLAN_ID = 4096
MAX_VLAN_ID = 4095

class VCPVlans(object):
    """The private-vlan object"""

    def __init__(self, vc_session):
        self._vlan_id_bits = BitArray(MAX_VLAN_ID + 1)
        self._init_pvlans(vc_session)
    #end __init__()

    def _init_pvlans(self, session):
        self._vlan_id_bits.set(1)

        dvs_mor = vm_util.get_dvs_ref_from_name(session, 
                                                CONF.vmware.vcenter_dvswitch);
        config_ret = session._call_method(vim_util,
                                             "get_dynamic_property",
                                             dvs_mor,
                                             "VmwareDistributedVirtualSwitch",
                                             "config")
        if not config_ret:
            return

        for pvlan_map_entry in config_ret.pvlanConfig:
            if pvlan_map_entry.pvlanType == "isolated":
                self._vlan_id_bits[pvlan_map_entry.secondaryVlanId] = 0

        port_grps_mor_ret = session._call_method(vim_util,
                                             "get_dynamic_property",
                                             dvs_mor,
                                             "VmwareDistributedVirtualSwitch",
                                             "portgroup")
        if not port_grps_mor_ret:
            return

        port_grps_mor = port_grps_mor_ret.ManagedObjectReference

        for port_grp_mor in port_grps_mor:
            cfg_ret = session._call_method(vim_util,
                                                 "get_dynamic_property",
                                                 port_grp_mor,
                                                 "DistributedVirtualPortgroup",
                                                 "config")
            if not cfg_ret:
                continue;

            if not cfg_ret.defaultPortConfig:
                continue;

            if not cfg_ret.defaultPortConfig.vlan:
                continue;

            if hasattr(cfg_ret.defaultPortConfig.vlan, "pvlanId"):
                self._vlan_id_bits[cfg_ret.defaultPortConfig.vlan.pvlanId] = 1

    #end _init_vlans()

    def alloc_pvlan(self):
        vid = self._vlan_id_bits.find('0b0')
        if vid:
            self._vlan_id_bits[vid[0]] = 1
            return vid[0]
        return INVALID_VLAN_ID
    #end alloc_vlan()

    def free_pvlan(self, vlan_id):
        if vlan_id < 0 or vlan_id > MAX_VLAN_ID:
            return
        self._vlan_id_bits[vlan_id] = 0
    #end free_vlan()

#end VCPVlans


class ContrailVCDriver(VMwareVCDriver):
    """Sub class of VC"""

    def __init__(self, virtapi, read_only=False, scheme="https"):
        super(ContrailVCDriver, self).__init__(virtapi)
        if CONF.vmware.vcenter_dvswitch is None:
            raise Exception(_("Must specify vcenter_dvswitch, to use "
                              "compute_driver=vmwareapi.contrailVCDriver"))
        self.Vlan = VCPVlans(self._session)

    def remove_dvport_group(self, name):
        session = self._session
        try:
            dvpg_mor = vm_util.get_dvportgroup_ref_from_name(session, name)
            try:
                destroy_task = self._session._call_method(
                    session.vim,
                    "Destroy_Task", dvpg_mor)
                self._session._wait_for_task(destroy_task)
            except Exception as excep:
                LOG.warn(_("In contrailVC:dvportgroup:delete, got this exception"
                           " while destroying the dvportGroup: %s") % str(excep))
        except Exception as exc:
            LOG.exception(exc)

    def spawn(self, context, instance, image_meta, injected_files,
              admin_password, network_info=None, block_device_info=None):
        session = self._session
        first_cluster = self._resources.keys()[0]
        if network_info:
            for vif in network_info:
                network_uuid = vif['network']['id']
                network_mor = vm_util.get_dvportgroup_ref_from_name(session, network_uuid)

                if not network_mor:
                    vif['network']['bridge'] = vif['network']['id']
                    pvlan_id = self.Vlan.alloc_pvlan()
                    if pvlan_id == INVALID_VLAN_ID:
                        raise exception.NovaException("Vlan id space is full")

                    try:
                        network_util.create_dvsport_group(session,
                                                    network_uuid,
                                                    CONF.vmware.vcenter_dvswitch,
                                                    pvlan_id, first_cluster)
                    except vexc.DuplicateName:
                        self.Vlan.free_pvlan(pvlan_id)
                else:
                    vif['network']['bridge'] = vif['network']['id']
                    #LOG.debug(_("Network %s found on host!") % network_uuid)

                args = {'should_create_vlan':False, 'vlan':'0'}
                vif['network']._set_meta(args)

        super(ContrailVCDriver, self).spawn(context, instance, image_meta,
                                             injected_files, admin_password,
                                             network_info, block_device_info)

    def destroy(self, instance, network_info, block_device_info=None,
                destroy_disks=True, context=None):
        super(ContrailVCDriver, self).destroy(instance, network_info,
                block_device_info, destroy_disks, context)

        if not network_info:
            return

        session = self._session

        for vif in network_info['info_cache']['network_info']:
            network_uuid = vif['network']['id']
            dvpg_mor = vm_util.get_dvportgroup_ref_from_name(session, network_uuid)
            if not dvpg_mor:
                continue;

            vms_ret = session._call_method(vim_util,
                                                 "get_dynamic_property",
                                                 dvpg_mor,
                                                 "DistributedVirtualPortgroup",
                                                 "vm")
            if not (vms_ret) or not (vms_ret.ManagedObjectReference):
                cfg_ret = session._call_method(vim_util,
                                              "get_dynamic_property",
                                              dvpg_mor,
                                              "DistributedVirtualPortgroup",
                                              "config")
                if not cfg_ret:
                    continue;

                if not cfg_ret.defaultPortConfig:
                    continue;

                if not cfg_ret.defaultPortConfig.vlan:
                    continue;

                if hasattr(cfg_ret.defaultPortConfig.vlan, "pvlanId"):
                    self.Vlan.free_pvlan(cfg_ret.defaultPortConfig.vlan.pvlanId)
                self.remove_dvport_group(network_uuid)

    def plug_vifs(self, instance, network_info):
        """Plug VIFs into networks."""
        # vif plug is done by vcenter plugin. Not done here.
        return

    def attach_interface(self, instance, image_meta, vif):
        """Attach an interface to the instance."""
        session = self._session
        first_cluster = self._resources.keys()[0]
        network_uuid = vif['network']['id']
        network_mor = vm_util.get_dvportgroup_ref_from_name(session, network_uuid)

        if not network_mor:
            vif['network']['bridge'] = vif['network']['id']
            pvlan_id = self.Vlan.alloc_pvlan()
            #LOG.debug(_("Allocated pvlan for network %s") % network_uuid)
            if pvlan_id == INVALID_VLAN_ID:
                raise exception.NovaException("Vlan id space is full")

            #LOG.debug(_("creating %s port-group on cluster!") % network_uuid)
            network_util.create_dvsport_group(session,
                                            network_uuid,
                                            CONF.vmware.vcenter_dvswitch,
                                            pvlan_id, first_cluster)
        else:
            vif['network']['bridge'] = vif['network']['id']
            #LOG.debug(_("Network %s found on host!") % network_uuid)

        args = {'should_create_vlan':False, 'vlan':'0'}
        vif['network']._set_meta(args)

        super(ContrailVCDriver, self).attach_interface(instance, image_meta, vif)

    def detach_interface(self, instance, vif):
        """Detach an interface from the instance."""
        super(ContrailVCDriver, self).detach_interface(instance, vif)

        session = self._session
        network_uuid = vif['network']['id']
        dvpg_mor = vm_util.get_dvportgroup_ref_from_name(session, network_uuid)
        if not dvpg_mor:
            return;

        vms_ret = session._call_method(vim_util,
                                       "get_dynamic_property",
                                       dvpg_mor,
                                       "DistributedVirtualPortgroup",
                                       "vm")
        if not (vms_ret) or (vms_ret.ManagedObjectReference):
            cfg_ret = session._call_method(vim_util,
                                          "get_dynamic_property",
                                          dvpg_mor,
                                          "DistributedVirtualPortgroup",
                                          "config")
            if not cfg_ret:
                return;

            if not cfg_ret.defaultPortConfig:
                return;

            if not cfg_ret.defaultPortConfig.vlan:
                return;

            if hasattr(cfg_ret.defaultPortConfig.vlan, "pvlanId"):
                self.Vlan.free_pvlan(cfg_ret.defaultPortConfig.vlan.pvlanId)

            self.remove_dvport_group(network_uuid)

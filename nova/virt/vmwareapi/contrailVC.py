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

from oslo_config import cfg
from oslo_vmware import vim_util as oslo_vmware_vim_util
from oslo_vmware import exceptions as vexc
from oslo_log import log as logging
from oslo_utils import uuidutils
from nova import exception
#from nova.openstack.common import loopingcall
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
        config_ret = session._call_method(oslo_vmware_vim_util,
                                             "get_object_property",
                                             dvs_mor,
                                             "config")
        if not config_ret:
            return

        for pvlan_map_entry in config_ret.pvlanConfig:
            if pvlan_map_entry.pvlanType == "isolated":
                self._vlan_id_bits[pvlan_map_entry.secondaryVlanId] = 0

        port_grps_mor_ret = session._call_method(oslo_vmware_vim_util,
                                             "get_object_property",
                                             dvs_mor,
                                             "portgroup")
        if not port_grps_mor_ret:
            return

        port_grps_mor = port_grps_mor_ret.ManagedObjectReference

        for port_grp_mor in port_grps_mor:
            cfg_ret = session._call_method(oslo_vmware_vim_util,
                                                 "get_object_property",
                                                 port_grp_mor,
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

    def spawn(self, context, instance, image_meta, injected_files,
              admin_password, network_info=None, block_device_info=None):
        session = self._session
        if network_info:
            for vif in network_info:
                network_uuid = vif['network']['id']
                #For Mitaka multicluster support appending cluster id to port group
                separator = u'_'
                network_uuid =  separator.join((self._vmops._cluster.value.encode('utf8'),
                                                network_uuid))
                network_ref=None
                try:
                    network_ref = network_util.get_network_with_the_name(session,
                                                   network_uuid, self._vmops._cluster)
                except vexc.VMwareDriverException:
                    LOG.debug("Network %s not found on cluster!", network_uuid)

                if not network_ref:
                    vif['network']['bridge'] = network_uuid
                    pvlan_id = self.Vlan.alloc_pvlan()
                    if pvlan_id == INVALID_VLAN_ID:
                        raise exception.NovaException("Vlan id space is full")

                    try:
                        network_util.create_dvport_group(session,
                                                    network_uuid,
                                                    CONF.vmware.vcenter_dvswitch,
                                                    pvlan_id, self._vmops._cluster)
                    except vexc.DuplicateName:
                        self.Vlan.free_pvlan(pvlan_id)
                else:
                    vif['network']['bridge'] = network_uuid
                    #LOG.debug(_("Network %s found on host!") % network_uuid)

                args = {'should_create_vlan':False, 'vlan':'0'}
                vif['network']._set_meta(args)

        super(ContrailVCDriver, self).spawn(context, instance, image_meta,
                                             injected_files, admin_password,
                                             network_info, block_device_info)

    def destroy(self, context, instance, network_info, block_device_info=None,
                destroy_disks=True, migrate_data=None):
        session = self._session

        super(ContrailVCDriver, self).destroy(context, instance, network_info,
                                              block_device_info, destroy_disks,
                                              migrate_data)

        if not network_info:
            return

        for vif in network_info:
            network_uuid = vif['network']['id']
            #For Mitaka multicluster support appending cluster id to port group
            separator = u'_'
            network_uuid =  separator.join((self._vmops._cluster.value.encode('utf8'),
                                            network_uuid))
            network_ref=None
            try:
                network_ref = network_util.get_network_with_the_name(session,
                                                     network_uuid, self._vmops._cluster);
            except vexc.VMwareDriverException:
                LOG.debug("Network %s not found on cluster!", network_uuid)

            if not network_ref:
                continue;

            dvpg_mor = oslo_vmware_vim_util.get_moref(network_ref['dvpg'], network_ref['type']);

            vms_ret = session._call_method(oslo_vmware_vim_util,
                                                 "get_object_property",
                                                 dvpg_mor,
                                                 "vm")
            if not (vms_ret) or not (vms_ret.ManagedObjectReference):
                cfg_ret = session._call_method(oslo_vmware_vim_util,
                                              "get_object_property",
                                              dvpg_mor,
                                              "config")
                if (cfg_ret) and (cfg_ret.defaultPortConfig) and (cfg_ret.defaultPortConfig.vlan) :
                    if hasattr(cfg_ret.defaultPortConfig.vlan, "pvlanId"):
                        pvlan_id = cfg_ret.defaultPortConfig.vlan.pvlanId

                # No VM exists on the dvPortGroup. Delete the network.
                # Only if delete network is successful, free pvlan.
                # This is to avoid it getting reused when old
                # network is hanging around due to failed n/w delete operation
                try:
                    network_util.delete_dvport_group(session, dvpg_mor)
                    if pvlan_id:
                        self.Vlan.free_pvlan(pvlan_id)
                except Exception as excep:
                    LOG.warn(("In contrailVC:dvportgroup:delete, "
                               "got this exception while destroying "
                               "the dvportGroup: %s") % str(excep))

    def plug_vifs(self, instance, network_info):
        """Plug VIFs into networks."""
        # vif plug is done by vcenter plugin. Not done here.
        return

    def attach_interface(self, instance, image_meta, vif):
        """Attach an interface to the instance."""
        session = self._session
        network_uuid = vif['network']['id']
        #For Mitaka multicluster support appending cluster id to port group
        separator = u'_'
        network_uuid =  separator.join((self._vmops._cluster.value.encode('utf8'),
                                        network_uuid))
        network_mor=None
        try:
            network_mor = network_util.get_network_with_the_name(session,
                                                   network_uuid, self._vmops._cluster);
        except vexc.VMwareDriverException:
            LOG.debug("Network %s not found on cluster!", network_uuid)

        if not network_mor:
            vif['network']['bridge'] = network_uuid
            pvlan_id = self.Vlan.alloc_pvlan()
            #LOG.debug(_("Allocated pvlan for network %s") % network_uuid)
            if pvlan_id == INVALID_VLAN_ID:
                raise exception.NovaException("Vlan id space is full")

            #LOG.debug(_("creating %s port-group on cluster!") % network_uuid)
            network_util.create_dvport_group(session,
                                            network_uuid,
                                            CONF.vmware.vcenter_dvswitch,
                                            pvlan_id, _vmops._cluster)
        else:
            vif['network']['bridge'] = network_uuid
            #LOG.debug(_("Network %s found on host!") % network_uuid)

        args = {'should_create_vlan':False, 'vlan':'0'}
        vif['network']._set_meta(args)

        super(ContrailVCDriver, self).attach_interface(instance, image_meta, vif)

    def detach_interface(self, instance, vif):
        """Detach an interface from the instance."""
        super(ContrailVCDriver, self).detach_interface(instance, vif)

        session = self._session
        network_uuid = vif['network']['id']
        #For Mitaka multicluster support appending cluster id to port group
        separator = u'_'
        network_uuid =  separator.join((self._vmops._cluster.value.encode('utf8'),
                                        network_uuid))
        dvpg_mor=None
        try:
            dvpg_mor = network_util.get_network_with_the_name(session, 
                                                   network_uuid, self._vmops._cluster);
        except vexc.VMwareDriverException:
            LOG.debug("Network %s not found on cluster!", network_uuid)

        if not dvpg_mor:
            return;

        vms_ret = session._call_method(oslo_vmware_vim_util,
                                       "get_object_property",
                                       dvpg_mor,
                                       "vm")
        if not (vms_ret) or (vms_ret.ManagedObjectReference):
            cfg_ret = session._call_method(oslo_vmware_vim_util,
                                          "get_object_property",
                                          dvpg_mor,
                                          "config")
            if not cfg_ret:
                return;

            if not cfg_ret.defaultPortConfig:
                return;

            if not cfg_ret.defaultPortConfig.vlan:
                return;

            if hasattr(cfg_ret.defaultPortConfig.vlan, "pvlanId"):
                self.Vlan.free_pvlan(cfg_ret.defaultPortConfig.vlan.pvlanId)

            self.remove_dvport_group(dvpg_mor)
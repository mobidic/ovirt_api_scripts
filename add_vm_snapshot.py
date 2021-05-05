#!/usr/bin/python

#
# Copyright (c) 2016 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import argparse
import datetime
import ovirtsdk4 as sdk
import ovirtsdk4.types as types

import config

logging.basicConfig(level=logging.DEBUG, filename='example.log')


def log(level, text):
    localtime = time.asctime(time.localtime(time.time()))
    if level == 'ERROR':
        sys.exit('[{0}]: {1} - {2}'.format(level, localtime, text))
    print('[{0}]: {1} - {2}'.format(level, localtime, text))

def snapshot(vms_service, vm, current_date):
    log('INFO', 'Snapshoting VM: {0}'.format(vm.name))
    # Locate the service that manages the snapshots of the virtual machine:
    snapshots_service = vms_service.vm_service(vm.id).snapshots_service()

    # Add the new snapshot:
    snapshots_service.add(
        types.Snapshot(
            description='{0}_{1}'.format(vm.name, current_date),
        ),
    )


def export_ova(connection, vms_service, vm, arch_type, current_date):
    log('INFO', 'Exporting OVA of VM: {0}'.format(vm.name))
    # shut down the VM before exporting?
    
    # OVA export
    vm_service = vms_service.vm_service(vm.id)
    # Find the host:
    myhost = config.odev_host if arch_type = 'odev' else config.ovirt_host
    hosts_service = connection.system_service().hosts_service()
    host = hosts_service.list(search='name={}'.format(myhost))[0]
    # Export the virtual machine. Note that the 'filename' parameter is
    # optional, and only required if you want to specify a name for the
    # generated OVA file that is different from <vm_name>.ova.
    # Note that this operation is only available since version 4.2 of
    # the engine and since version 4.2 of the SDK.
    vm_service.export_to_path_on_host(
        host=types.Host(id=host.id),
        directory=config.ova_export_dir,
        filename='{0}_{1}.ova'.format(vm.name, current_date)
    )


def main():
    parser = argparse.ArgumentParser(
        description='Checks for ANNOVAR resources distant updates and convert to ANNOVAR format',
        usage='python update_resources.py <-d clinvar> <-hp /path/to/annovar/humandb> <-g [GRCh37|GRCh38]> <-a path/to/annovar>'
    )
    parser.add_argument('-a', '--arch-type', default='odev', required=True,
        help='architecture to realise the snapshot on [odev|ovirt], default=odev')
    parser.add_argument('-n', '--name', required=False,
        help='name of VM to be snapshoted on')
    parser.add_argument('-t', '--backup-type', default='snapshot', required=True,
        help='backup type [snapshot|ova], default=snapshot')
    args = parser.parse_args()
    fqdn = config.odev_fqdn
    arch_type = 'odev'
    if args.arch_type and \
            args.arch_type == 'ovirt':
         fqdn = config.ovirt_fqdn
         arch_type = 'ovirt'
    name = 'all'
    if args.name:
        name = args.name
    btype = 'snapshot'
    if args.backup_type and \
            args.backup_type == 'ova':
        btype = 'ova'
    
    now = datetime.datetime.now()
    current_date = now.strftime("%Y%d%m")

    # Create the connection to the server:
    connection = sdk.Connection(
        url='https://{}/ovirt-engine/api'.format(fqdn),
        username='{}@internal'.format(config.login),
        password=config.password,
        ca_file=config.odev_ca_cert,
        debug=True,
        log=logging.getLogger(),
    )
    log('INFO', 'Connecting to the HostedEngin API of {}'.format(arch_type))
    # Locate the virtual machines service and use it to find the virtual
    # machine:
    vms_service = connection.system_service().vms_service()

    if name == 'all':
        vms = vms_service.list()
        
        for vm in vms:
            # not the HostedEngine
            if vn.name != 'HostedEngine':
                if btype == 'snapshot':
                    snapshot(vms_service, vm, current_date)
                    # log('INFO', 'Snapshoting VM: {0}'.format(vm.name))

                    # Locate the service that manages the snapshots of the virtual machine:
                    # snapshots_service = vms_service.vm_service(vm.id).snapshots_service()

                    # Add the new snapshot:
                    # snapshots_service.add(
                    #     types.Snapshot(
                    #         description='{0}_{1}'.format(vm.name, current_date),
                    #     ),
                    # )
                else:
                    export_ova(connection, vms_service, vm, arch_type, current_date)
                    # log('INFO', 'Exporting OVA for VM: {0}, status: {1}'.format(vm.name, vm.status))
                    # shut down the VM before exporting?

                    # OVA export
                    # vm_service = vms_service.vm_service(vm.id)
                    # Find the host:
                    # hosts_service = connection.system_service().hosts_service()
                    # host = hosts_service.list(search='name=myhost')[0]
    else:
        log('INFO', 'VM: {0}, status: {1}'.format(vm.name, vm.status))
        vm = vms_service.list(search='name={}'.format(name))[0]
        if vn.name != 'HostedEngine':
            if btype == 'snapshot':
                snapshot(vms_service, vm, current_date)
            else:
                export_ova(connection, vms_service, vm, arch_type, current_date)

    # Close the connection to the server:
    connection.close()


if __name__ == '__main__':
        main()

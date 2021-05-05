import logging
import argparse
import datetime
import ovirtsdk4 as sdk
import ovirtsdk4.types as types
# custom config
import config


# This script will trigger snapshots or OVA export on an ovirt architecture
# and a specific VM or all Vms present except the HostedEngine


logging.basicConfig(level=logging.DEBUG, filename='example.log')


def log(level, text):
    localtime = datetime.datetime.now()
    # localtime = time.asctime(time.localtime(time.time()))
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
    log('INFO', 'Snapshot ended for VM: {0}'.format(vm.name))


def export_ova(connection, vms_service, vm, arch_type, current_date):
    log('INFO', 'Exporting OVA of VM: {0}'.format(vm.name))
    # shut down the VM before exporting?
    
    # OVA export
    vm_service = vms_service.vm_service(vm.id)
    # Find the host:
    myhost = config.odev_host if arch_type == 'odev' else config.ovirt_host
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
    log('INFO', 'OVA export ended for VM: {0}'.format(vm.name))


def main():
    parser = argparse.ArgumentParser(
        description='Connects to ovirt and triggers ova export or snaphots',
        usage='python3 trigger_backup.py <-a [odev|ovirt]> <-n vmname> <-t [snapshot|ova]>'
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
                else:
                    export_ova(connection, vms_service, vm, arch_type, current_date)
    else:
        vm = vms_service.list(search='name={}'.format(name))[0]
        if vm.name != 'HostedEngine':
            if btype == 'snapshot':
                snapshot(vms_service, vm, current_date)
            else:
                export_ova(connection, vms_service, vm, arch_type, current_date)

    # Close the connection to the server:
    connection.close()


if __name__ == '__main__':
        main()

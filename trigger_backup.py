import sys
import re
import logging
import argparse
import datetime
import time
import ovirtsdk4 as sdk
import ovirtsdk4.types as types
# custom config
import config


# This script will trigger snapshots or OVA export on an ovirt architecture
# and a specific VM or all Vms present except the HostedEngine


def log(level, text):
    localtime = datetime.datetime.now()
    # localtime = time.asctime(time.localtime(time.time()))
    if level == 'ERROR':
        sys.exit('[{0}]: {1} - {2}'.format(level, localtime, text))
    print('[{0}]: {1} - {2}'.format(level, localtime, text))


def snapshot(vms_service, vm, current_date, keep_memory):
    snap_type = 'nightly' if keep_memory is False else 'weekly'
    logging.basicConfig(
        level=logging.INFO,
        filename='logs/{0}_{1}_backup.log'.format(vm.name, snap_type),
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    log('INFO', 'Snapshoting VM: {0}'.format(vm.name))
    # Locate the service that manages the snapshots of the virtual machine:
    snapshots_service = vms_service.vm_service(vm.id).snapshots_service()

    # Add the new snapshot:
    snap = snapshots_service.add(
        types.Snapshot(
            description='{0}_{1}_{2}'.format(current_date, snap_type, vm.name),
            persist_memorystate=keep_memory,
        ),
    )
    logging.info(
        'Sent request to create snapshot \'{0}\', the id is \'{1}\'.'.format(snap.description, snap.id),
    )
    # Poll and wait till the status of the snapshot is 'ok', which means
    # that it is completely created:
    snap_service = snapshots_service.snapshot_service(snap.id)
    while snap.snapshot_status != types.SnapshotStatus.OK:
        logging.info(
            'Waiting till the snapshot is created, the satus is now \'{0}\'.'.format(snap.snapshot_status)
        )
        time.sleep(5)
        snap = snap_service.get()
    logging.info('The snapshot is now complete.')
    log('INFO', 'Snapshot ended for VM: {0}'.format(vm.name))
    # keep only the 5 more recent if keep_memory is False => Nightly
    if keep_memory is False:
        remove_oldest_snapshot(snapshots_service, snap_type, 5, logging)
    else:
        # keep only the 4 more recent if keep_memory is True
        remove_oldest_snapshot(snapshots_service, snap_type, 4, logging)


def remove_oldest_snapshot(snapshots_service, snap_type, nb, logging):
    snaps_map = {
        snap.id: snap.description
        for snap in snapshots_service.list()
    }
    # .iteritems()  sorted(snaps_map, reverse=True)
    log('DEBUG', 'snaps_map: {}'.format(snaps_map))
    nb_snap = 0
    for snap_id, snap_description in sorted(snaps_map.items(), reverse=True):
        match_obj = re.search(rf'^\d{8}_{snap_type}', snap_description)
        if match_obj:
            # oldest last
            # log('DEBUG', 'snapshot {}'.format(snap_description))
            nb_snap += 1
            if nb_snap > nb:
                # Remove the snapshot:
                snap_service = snapshots_service.snapshot_service(snap_id)
                logging.info('Removing snapshot {0}, id: {1}'.format(snap_description, snap_id))
                log('INFO', 'Removing snapshot {}'.format(snap_description))
                snap_service.remove()


def export_ova(connection, vms_service, vm, arch_type, current_date):
    logging.basicConfig(
        level=logging.INFO,
        filename='logs/{0}_OVA_backup.log'.format(vm.name),
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    log('INFO', 'Exporting OVA of VM: {0}'.format(vm.name))
    # OVA export
    vm_service = vms_service.vm_service(vm.id)
    # shut down the VM before exporting?
    # if vm.status == types.VmStatus.UP:
    #     # Call the "stop" method of the service to stop it:
    #     logging.info('Sent request to shut down VM {}.'.format(vm.name))
    #     vm_service.stop()
    #     # Wait till the virtual machine is down:
    #     while True:
    #         time.sleep(5)
    #         vm = vm_service.get()
    #         if vm.status == types.VmStatus.DOWN:
    #             break
    # logging.info('VM {0} state: {1}.'.format(vm.name, vm.status))
    # if vm.status == types.VmStatus.DOWN:
    # Find the host:
    myhost = config.odev_host if arch_type == 'odev' else config.ovirt_host
    hosts_service = connection.system_service().hosts_service()
    host = hosts_service.list(search='name={}'.format(myhost))[0]
    # Export the virtual machine. Note that the 'filename' parameter is
    # optional, and only required if you want to specify a name for the
    # generated OVA file that is different from <vm_name>.ova.
    # Note that this operation is only available since version 4.2 of
    # the engine and since version 4.2 of the SDK.
    logging.info(
        'Sent request to create OVA for {0} to host {1} and path {2}.'.format(
            vm.name, host.name, config.ova_export_dir
        )
    )
    ova_export = vm_service.export_to_path_on_host(
        host=types.Host(id=host.id),
        directory=config.ova_export_dir,
        filename='{0}_{1}.ova'.format(current_date, vm.name),
        wait=True
    )
    # wait = True => wait for validation, not ova export
    # wait for OVA export to finish
    log('DEBUG', 'OVA export object: {}'.format(ova_export))
    # vm.status is down during export_to_path_on_host
    # how to get export status?????
    # while vm.status == types.VmStatus.IMAGE_LOCKED:
    #     time.sleep(20)
    #     vm = vm_service.get()
    # logging.info('Sent request to start {0}.'.format(vm.name))
    # # Call the "start" method of the service to start it:
    # vm_service.start()
    # # Wait till the virtual machine is up:
    # while True:
    #     time.sleep(5)
    #     vm = vm_service.get()
    #     if vm.status == types.VmStatus.UP:
    #         break
    # logging.info('VM {0} state: {1}.'.format(vm.name, vm.status))
    log('INFO', 'OVA export started for VM: {0}'.format(vm.name))
    # else:
    #     logging.WARNING('VM {0} did not stop - OVA export canceled. Current status: {1}'.format(vm.name, vm.status))


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
    parser.add_argument('-km', '--keep-memory', default=False, required=False,
                        help='if -t snapshot, includes RAM state or not', action='store_true')
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
    if btype == 'snapshot':
        keep_memory = True if args.keep_memory else False

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
    log('INFO', 'Connecting to the HostedEngine API of {}'.format(arch_type))
    # Locate the virtual machines service and use it to find the virtual
    # machine:
    vms_service = connection.system_service().vms_service()

    if name == 'all':
        vms = vms_service.list()

        for vm in vms:
            # not the HostedEngine
            if vm.name != 'HostedEngine':
                if btype == 'snapshot':
                    snapshot(vms_service, vm, current_date, keep_memory)
                else:
                    export_ova(connection, vms_service, vm, arch_type, current_date)
    else:
        vm = vms_service.list(search='name={}'.format(name))[0]
        if vm.name != 'HostedEngine':
            if btype == 'snapshot':
                snapshot(vms_service, vm, current_date, keep_memory)
            else:
                export_ova(connection, vms_service, vm, arch_type, current_date)

    # Close the connection to the server:
    connection.close()


if __name__ == '__main__':
    main()

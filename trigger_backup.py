import os
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


def check_descriptions(snapshots_service, current_date, snap_type, vm_name):
    i = 0
    desc_to_test = '{0}_{1}_{2}'.format(current_date, snap_type, vm_name)
    for snap in snapshots_service.list():
        if snap.description == desc_to_test:
            i += 1
            desc_to_test = '{0}_{1}_{2}_{3}'.format(current_date, snap_type, vm_name, i)
    return desc_to_test
    # may not work if snapshots_service.list() is not well sorted ...


def snapshot(vms_service, vm, current_date, arch_type, keep_memory):
    snap_type = 'nightly' if keep_memory is False else 'weekly'
    logging.basicConfig(
        format="%(asctime)-15s [%(levelname)s] %(message)s",
        filemode='w',
        level=logging.INFO,
        filename='{0}/logs/{1}_{2}_backup.log'.format(os.path.dirname(__file__), vm.name, snap_type),
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    log('INFO', 'Snapshoting VM: {0}'.format(vm.name))
    # Locate the service that manages the snapshots of the virtual machine:
    snapshots_service = vms_service.vm_service(vm.id).snapshots_service()
    # get descritpions to ensure we have a unique one
    snapshot_desc = check_descriptions(snapshots_service, current_date, snap_type, vm.name)

    # Add the new snapshot:
    snap = snapshots_service.add(
        types.Snapshot(
            description=snapshot_desc,
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
        time.sleep(5)
        snap = snap_service.get()
    logging.info('The snapshot is now complete.')
    log('INFO', 'Snapshot ended for VM: {0}'.format(vm.name))
    # keep only the 5 more recent if keep_memory is False => Nightly
    if keep_memory is False:
        number = config.ovirt_nightly_snapshot_nb if arch_type == 'ovirt' else config.odev_nightly_snapshot_nb
        remove_oldest_snapshot(snapshots_service, snap_type, number, logging)
    else:
        # keep only the 4 more recent if keep_memory is True
        number = config.ovirt_weekly_snapshot_nb if arch_type == 'ovirt' else config.odev_weekly_snapshot_nb
        remove_oldest_snapshot(snapshots_service, snap_type, number, logging)


def remove_oldest_snapshot(snapshots_service, snap_type, nb, logging):
    snaps_map = {
        snap.id: snap.description
        for snap in snapshots_service.list()
    }
    # .iteritems()  sorted(snaps_map, reverse=True)
    # log('DEBUG', 'snaps_map: {}'.format(snaps_map))
    nb_snap = 0
    # put keys and values in list to get the index later
    snap_ids = list(snaps_map.keys())
    snap_descriptions = list(snaps_map.values())
    # for snap_id, snap_description in sorted(snaps_map.items(), reverse=True):
    # we iterate on reverse values (descriptions) to get the oldest dates last
    # a little bit heavy but we want to control the order
    # This does not work with multiple descriptions at the same date
    for snap_description in sorted(snaps_map.values(), reverse=True):
        # get associated key
        snap_description_index = snap_descriptions.index(snap_description)
        snap_id = snap_ids[snap_description_index]
        match_obj = re.search(rf'^\d{{8}}_{snap_type}_', snap_description)
        if match_obj:
            # oldest last
            # log('DEBUG', 'snapshot {0}, id: {1}'.format(snap_description, snap_id))
            nb_snap += 1
            if nb_snap > nb:
                # Remove the snapshot:
                snap_service = snapshots_service.snapshot_service(snap_id)
                logging.info('Removing snapshot {0}, id: {1}'.format(snap_description, snap_id))
                log('INFO', 'Removing snapshot {0}, id: {1}'.format(snap_description, snap_id))
                try:
                    snap_service.remove()
                except Exception:
                    log('WARNING', 'Cannot find snap_service for snap id:{0}'.format(snap_id))
                # wait till the we can't ask snap_servcice.get(), which means
                # that removal is done:
                snap = snap_service.get()
                while snap.snapshot_status == types.SnapshotStatus.LOCKED:
                    time.sleep(5)
                    try:
                        snap = snap_service.get()
                    except Exception as e:
                        if not re.search(r'404', str(e.args)):
                            log('DEBUG', str(e.args))
                            logging.warning('Removing snapshot for desc {0} failed with error {1}'.format(snap_description, e.args))
                        break
                log('INFO', 'Snapshot removal ended for snapshot: {0}'.format(snap_description))
                logging.info('Removed snapshot {0}, id: {1}'.format(snap_description, snap_id))


def export_ova(connection, vms_service, vm, arch_type, current_date):
    logging.basicConfig(
        format="%(asctime)-15s [%(levelname)s] %(message)s",
        filemode='w',
        level=logging.INFO,
        filename='{0}/logs/{1}_ova_backup.log'.format(os.path.dirname(__file__), vm.name),
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
        filename='{0}_{1}_{2}.ova'.format(current_date, arch_type, vm.name),
        wait=True
    )
    # wait = True => wait for validation, not ova export
    # wait for OVA export to finish
    # log('DEBUG', 'OVA export object: {}'.format(ova_export))
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
    cacert = config.odev_ca_cert
    arch_type = 'odev'
    if args.arch_type and \
            args.arch_type == 'ovirt':
        fqdn = config.ovirt_fqdn
        cacert = config.ovirt_ca_cert
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
    current_date = now.strftime("%Y%m%d")
    # Create the connection to the server:
    connection = sdk.Connection(
        url='https://{}/ovirt-engine/api'.format(fqdn),
        username='{}@internal'.format(config.login),
        password=config.password,
        ca_file=cacert,
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
                    snapshot(vms_service, vm, current_date, arch_type, keep_memory)
                else:
                    export_ova(connection, vms_service, vm, arch_type, current_date)
    else:
        vm = vms_service.list(search='name={}'.format(name))[0]
        if vm.name != 'HostedEngine':
            if btype == 'snapshot':
                snapshot(vms_service, vm, current_date, arch_type, keep_memory)
            else:
                export_ova(connection, vms_service, vm, arch_type, current_date)

    # Close the connection to the server:
    connection.close()


if __name__ == '__main__':
    main()

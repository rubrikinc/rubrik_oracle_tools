import rbs_oracle_common
import click
import logging
import sys


@click.command()
@click.option('--database', '-d', type=str, required=True,  help='The mounted databases name.')
@click.option('--target_host', '-m', type=str, required=True,  help='The target host with the live mount to remove.')
@click.option('--force', '-f', is_flag=True, help='Force unmount')
@click.option('--all_mounts', '-a', is_flag=True, help='Unmount all mounts of the database on the target host.')
@click.option('--id_unmount', '-i', help='Unmount a specific mount using the mount id. Multiple ids seperated by commas. ')
@click.option('--no_wait', is_flag=True, help='Queue Live Mount and exit.')
@click.option('--debug_level', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING or CRITICAL.')
def cli(database, target_host, force, all_mounts, id_unmount, no_wait,  debug_level):
    """
    Unmount a Rubrik database or files live mount using the database name and the live mount host.

    This will unmount a live mount of an Oracle database or a mount of the RMAN backup files. The source database is
    specified in a host:db format. Note that is the original database not the object to be unmounted. The mounted host
    is the host where the live mounted database or the mounted RMAN backup files will be unmounted. This works if there
    is only one live mounted database or set of backup files mounted on the host. If there is more than one, you can
    choose to unmount all the mounts on that host (-a) or specify a specific mount (-i) to unmount. You can list the
    mounts on a host using rubrik_oracle_mount_info.

    Returns:
        unmount_info (dict): Status of the unmount request.
    """
    numeric_level = getattr(logging, debug_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: {}'.format(debug_level))
    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(numeric_level)
    console_formatter = logging.Formatter('%(asctime)s: %(message)s')
    ch.setFormatter(console_formatter)
    logger.addHandler(ch)

    rubrik = rbs_oracle_common.RubrikConnection()
    if id_unmount:
        id_unmount = id_unmount.split(",")
    else:
        id_unmount = []
    logger.info("Checking for live mounts of source db {} on host {}".format(database, target_host))
    mount = rbs_oracle_common.RubrikRbsRplOracleMount(rubrik, database, target_host)
    live_mount_ids = mount.get_oracle_live_mount_id()
    logger.debug("Live Mount IDs found: {}".format(live_mount_ids))
    unmount_info = []
    if not live_mount_ids:
        rubrik.delete_session()
        raise RubrikOracleUnmountError(
            "No live mounts found for {} live mounted on {}. ".format(database, target_host))
    elif len(live_mount_ids) == 0:
        rubrik.delete_session()
        raise RubrikOracleUnmountError(
            "No live mounts found for {} live mounted on {}. ".format(database, target_host))
    elif len(live_mount_ids) == 1:
        logger.warning("Found live mount id: {} on {}".format(live_mount_ids[0], target_host))
        logger.warning("Deleting live mount.")
        delete_request = mount.live_mount_delete(live_mount_ids[0], force)
        if no_wait:
            logger.warning("Live mount id: {} Unmount status: {}.".format(live_mount_ids[0], delete_request['status']))
        else:
            # delete_request = mount.async_requests_wait(delete_request['id'], 12)
            delete_request = rbs_oracle_common.RubrikRbsOracleDatabase.async_requests_wait(delete_request['id'], 12)
            logger.warning("Async request completed with status: {}".format(delete_request['status']))
            logger.debug(delete_request)
        unmount_info.append(delete_request)
    elif len(live_mount_ids) > 1 and all_mounts:
        logger.warning("Delete all mounts is set to {}. Deleting all mounts on {}".format(all_mounts, target_host))
        for live_mount_id in live_mount_ids:
            logger.debug(live_mount_id)
            logger.warning("Deleting live mount with id: {} on {}".format(live_mount_ids[0], target_host))
            delete_request = mount.live_mount_delete(live_mount_id, force)
            if no_wait:
                logger.warning(
                    "Live mount id: {} Unmount status: {}.".format(live_mount_ids[0], delete_request['status']))
            else:
                delete_request = rbs_oracle_common.RubrikRbsOracleDatabase.async_requests_wait(delete_request['id'], 12)
                logger.warning("Async request completed with status: {}".format(delete_request['status']))
                logger.debug(delete_request)
            unmount_info.append(delete_request)
    elif len(live_mount_ids) > 1 and id_unmount:
        logger.info("Will delete the following mounts: {} on {}".format(id_unmount, mounted_host))
        for live_mount_id in live_mount_ids:
            if live_mount_id in id_unmount:
                logger.debug(live_mount_id)
                logger.warning("Deleting live mount with id: {} on {}".format(live_mount_ids[0], mounted_host))
                delete_request = mount.live_mount_delete(live_mount_id, force)
                if no_wait:
                    logger.warning(
                        "Live mount id: {} Unmount status: {}.".format(live_mount_ids[0], delete_request['status']))
                else:
                    delete_request = rbs_oracle_common.RubrikRbsOracleDatabase.async_requests_wait(delete_request['id'], 12)
                    logger.warning("Async request completed with status: {}".format(delete_request['status']))
                    logger.debug(delete_request)
                unmount_info.append(delete_request)
    else:
        raise RubrikOracleUnmountError( "Multiple live mounts found for {} live mounted on {}. "
                                            "Use --all_mounts or --id_mounts to unmount all or some of the mounts "
                                            .format(source_host_db[1], mounted_host))
    rubrik.delete_session()
    return unmount_info


class RubrikOracleUnmountError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()

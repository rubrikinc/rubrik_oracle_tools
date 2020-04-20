import rbs_oracle_common
import click
import logging
import sys


@click.command()
@click.option('--source_host_db', '-s', type=str, required=True,  help='The source <host or RAC cluster>:<database>')
@click.option('--mounted_host', '-m', type=str, required=True,  help='The host with the live mount to remove')
@click.option('--force', '-f', is_flag=True, help='Force unmount')
@click.option('--all_mounts', '-a', is_flag=True, help='Unmount all mounts from the source host:db. Provide all the clone names separated by commas.')
@click.option('--id_unmount', '-i', help='Unmount a specific mount using the mount id. Multiple ids seperated by commas. ')
@click.option('--no_wait', is_flag=True, help='Queue Live Mount and exit.')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING or CRITICAL.')
def cli(source_host_db, mounted_host, force, all_mounts, id_unmount, no_wait,  debug_level):
    """
    This will unmount a Rubrik live mount using the database name and the live mount host.

\b
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
    source_host_db = source_host_db.split(":")
    logger.info("Checking for live mounts of source db {} on host {}".format(source_host_db[1], mounted_host))
    mount = rbs_oracle_common.RubrikRbsOracleMount(rubrik, source_host_db[1], source_host_db[0], mounted_host)
    live_mount_ids = mount.get_oracle_live_mount_id()
    unmount_info = []
    if not live_mount_ids:
        raise RubrikOracleUnmountError("No live mounts found for {} live mounted on {}. ".format(source_host_db[1], mounted_host))
    elif len(live_mount_ids) == 1:
        logger.warning("Found live mount id: {} on {}".format(live_mount_ids[0], mounted_host))
        logger.warning("Deleting 1 live mount.")
        delete_request = mount.live_mount_delete(live_mount_ids[0], force)
        if no_wait:
            logger.warning("Live mount id: {} Unmount status: {}.".format(live_mount_ids[0], delete_request['status']))
        else:
            delete_request = mount.async_requests_wait(delete_request['id'], 12)
            logger.warning("Async request completed with status: {}".format(delete_request['status']))
            logger.debug(delete_request)
        unmount_info.append(delete_request)
    elif len(live_mount_ids) > 1 and all_mounts:
        logger.warning("Delete all mounts is set to {}. Deleting all mounts on {}".format(all_mounts, mounted_host))
        for live_mount_id in live_mount_ids:
            logger.debug(live_mount_id)
            logger.warning("Deleting live mount with id: {} on {}".format(live_mount_ids[0], mounted_host))
            delete_request = mount.live_mount_delete(live_mount_id, force)
            if no_wait:
                logger.warning(
                    "Live mount id: {} Unmount status: {}.".format(live_mount_ids[0], delete_request['status']))
            else:
                delete_request = mount.async_requests_wait(delete_request['id'], 12)
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
                    delete_request = mount.async_requests_wait(delete_request['id'], 12)
                    logger.warning("Async request completed with status: {}".format(delete_request['status']))
                    logger.debug(delete_request)
                unmount_info.append(delete_request)
    else:
        raise RubrikOracleUnmountError( "Multiple live mounts found for {} live mounted on {}. "
                                            "Use --all_mounts or --id_mounts to unmount all or some of the mounts "
                                            .format(source_host_db[1], mounted_host))
    return unmount_info


class RubrikOracleUnmountError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()

import rbs_oracle_common
import click
import logging
import sys
import platform
import pytz
import datetime


@click.command()
@click.option('--source_host_db', '-s', type=str, required=True,  help='The source <host or RAC cluster>:<database>')
@click.option('--mounted_host', '-m', type=str, required=True,  help='The host with the live mount to remove')
@click.option('--new_oracle_name', '-n', required=True, type=str, help='Oracle database clone name. If unmounting more than one separate with commas.')
@click.option('--oracle_home', '-o', type=str, help='ORACLE_HOME path for the mounted database(s) if different than source database ORACLE_HOME')
@click.option('--all_mounts', '-a', is_flag=True, help='Unmount all mounts from the source host:db. Provide all the clone names separated by commas.')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING or CRITICAL.')
def cli(source_host_db, mounted_host, new_oracle_name, oracle_home, all_mounts, debug_level):
    """
    This will unmount a Rubrik live mount that has had the name changed after the live mount
     using the the live mount host:Original DB Name, new Oracle DB name and the ORACLE_HOME
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

    # Make sure this is being run on the target host
    if mounted_host.split('.')[0] != platform.uname()[1].split('.')[0]:
        raise RubrikOracleCloneUnmountError("This program must be run on the mounted host: {}".format(mounted_host))
    source_host_db = source_host_db.split(":")
    new_oracle_name = new_oracle_name.split(',')
    if source_host_db[1] in new_oracle_name:
        raise RubrikOracleCloneUnmountError("Requesting drop of source database. This is not allowed in case that database is running on this host. Please only use the clone database names for the databases to be removed.")
    rubrik = rbs_oracle_common.RubrikConnection()
    logger.info("Checking for live mounts of source db {} on host {}".format(source_host_db[1], mounted_host))
    mount = rbs_oracle_common.RubrikRbsOracleMount(rubrik, source_host_db[1], source_host_db[0], mounted_host)
    live_mount_ids = mount.get_oracle_live_mount_id()
    if not oracle_home:
        source_db_info = mount.get_oracle_db_info()
        oracle_home = source_db_info['oracleHome']
    force = True
    cluster_timezone = pytz.timezone(rubrik.timezone)
    utc = pytz.utc
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    if not live_mount_ids:
        raise RubrikOracleCloneUnmountError("No live mounts found for {} live mounted on {}. ".format(source_host_db[1], source_host_db[0]))
    elif len(live_mount_ids) == 0:
        raise RubrikOracleCloneUnmountError(
            "No live mounts found for {} live mounted on {}. ".format(source_host_db[1], source_host_db[0]))
    elif len(live_mount_ids) == 1:
        logger.warning("Found live mount id: {} on {}".format(live_mount_ids[0], mounted_host))
        logger.warning("Deleting live mount.")
        delete_request = mount.live_mount_delete(live_mount_ids[0], force)
        delete_request = mount.async_requests_wait(delete_request['id'], 12)
        logger.warning("Async request completed with status: {}".format(delete_request['status']))
        logger.debug(delete_request)
        if delete_request['status'] != "SUCCEEDED":
            logger.warning("Unmount of backup files failed with status: {}".format(delete_request['status']))
        else:
            logger.warning("Live mount of backup data files with id: {} has been unmounted.".format(live_mount_ids[0]))
        mount.oracle_db_clone_cleanup(new_oracle_name[0], oracle_home)
        logger.warning("Clone database {} has been dropped.".format(new_oracle_name[0]))
        return
    elif len(live_mount_ids) > 1 and all_mounts:
        logger.warning("Delete all mounts is set to {}. Deleting all mounts on {}".format(all_mounts, mounted_host))
        for live_mount_id in live_mount_ids:
            logger.debug(live_mount_id)
            logger.warning("Deleting live mount with id: {} on {}".format(live_mount_ids[0], mounted_host))
            delete_request = mount.live_mount_delete(live_mount_id, force)
            delete_request = mount.async_requests_wait(delete_request['id'], 12)
            logger.warning("Async request completed with status: {}".format(delete_request['status']))
            logger.debug(delete_request)
            if delete_request['status'] != "SUCCEEDED":
                logger.warning("Unmount of backup files failed with status: {}".format(delete_request['status']))
            else:
                logger.warning("Live mount of backup data files with id: {} has been unmounted.".format(live_mount_id))
        for name in new_oracle_name:
            mount.oracle_db_clone_cleanup(name, oracle_home)
            logger.warning("Clone database {} has been dropped.".format(name))
        return
    else:
        raise RubrikOracleCloneUnmountError( "Multiple live mounts found for source database {} live mounted on {}. "
                                            "Use --all_mounts to unmount all or some of the mounts "
                                            .format(source_host_db[1], mounted_host))


class RubrikOracleCloneUnmountError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()

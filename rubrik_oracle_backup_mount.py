import rbs_oracle_common
import click
import logging
import sys
import datetime
import pytz


@click.command()
@click.option('--source_host_db', '-s', type=str, required=True,  help='The source <host or RAC cluster>:<database>')
@click.option('--mount_path', '-m', type=str, required=True, help='The path used to mount the backup files')
@click.option('--time_restore', '-t', type=str, help='Point in time to mount the DB, format is YY:MM:DDTHH:MM:SS example 2019-01-01T20:30:15')
@click.option('--host_target', '-h', type=str, help='Host or RAC cluster name (RAC target required if source is RAC)  for the Live Mount ')
@click.option('--timeout', type=int, help='API Timeout value in seconds. Default is 180 seconds')
@click.option('--no_wait', is_flag=True, help='Queue Live Mount and exit.')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING, ERROR or CRITICAL.')
def cli(source_host_db, mount_path, time_restore, host_target, timeout, no_wait, debug_level):
    """
    This will mount the requested Rubrik Oracle backup set on the provided path.

\b
    The source database is specified in a host:db format. The mount path is required. If the restore time is not
    provided the most recent recoverable time will be used. The host for the mount can be specified if it is not it
    will be mounted on the source host. On Rubrik CDM versions prior to 5.2.1, the source database is on a RAC cluster
    the target must be a RAC cluster. On Rubrik CDM versions 5.2.1 and higher, if the source database is on RAC or
    single instance the target can be RAC or a single instance host.
    Returns:
        live_mount_info (dict): The information about the requested files only mount returned from the Rubrik CDM.
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
    source_host_db = source_host_db.split(":")
    database = rbs_oracle_common.RubrikRbsOracleDatabase(rubrik, source_host_db[1], source_host_db[0], timeout)
    oracle_db_info = database.get_oracle_db_info()
    logger.debug(oracle_db_info)
    if not host_target:
        host_target = source_host_db[0]
    # If the CDM version is pre 5.2.1 and the source database is on a RAC cluster the target must be a RAC cluster otherwise it will be an Oracle Host
    cdm_version = rubrik.version.split("-")[0].split(".")
    if int(cdm_version[0]) < 6 and int(cdm_version[1]) < 3 and (int(cdm_version[1]) < 2 or int(cdm_version[2]) < 1):
        logger.info("Cluster version {} is pre 5.2.1".format(cdm_version))
        if 'racName' in oracle_db_info.keys():
            if oracle_db_info['racName']:
                target_id = database.get_rac_id(rubrik.cluster_id, host_target)
            else:
                target_id = database.get_host_id(rubrik.cluster_id, host_target)
    else:
        logger.info("Cluster version {}.{}.{} is post 5.2.1".format(cdm_version[0], cdm_version[1], cdm_version[2]))
        target_id = database.get_target_id(rubrik.cluster_id, host_target)
    # Use the provided time or if no time has been provided use the teh most recent recovery point
    if time_restore:
        time_ms = database.epoch_time(time_restore, rubrik.timezone)
        logger.warning("Mounting backup pieces for a point in time restore to time: {}.". format(time_restore))
    else:
        logger.warning("Using most recent recovery point for mount.")
        time_ms = database.epoch_time(oracle_db_info['latestRecoveryPoint'], rubrik.timezone)
    logger.warning("Starting the mount of the requested {} backup pieces on {}.".format(source_host_db[1], host_target))
    live_mount_info = database.live_mount(target_id, time_ms, files_only=True, mount_path=mount_path)
    cluster_timezone = pytz.timezone(rubrik.timezone)
    utc = pytz.utc
    start_time = utc.localize(datetime.datetime.fromisoformat(live_mount_info['startTime'][:-1])).astimezone(cluster_timezone)
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    logger.info("Live mount requested at {}.".format(start_time.strftime(fmt)))
    logger.info("No wait flag is set to {}.".format(no_wait))
    if no_wait:
        logger.warning("Live mount id: {} Mount status: {}.".format(live_mount_info['id'], live_mount_info['status']))
        return live_mount_info
    else:
        live_mount_info = database.async_requests_wait(live_mount_info['id'], 12)
        logger.warning("Async request completed with status: {}".format(live_mount_info['status']))
        if live_mount_info['status'] != "SUCCEEDED":
            raise RubrikOracleBackupMountError(
                "Mount of backup files did not complete successfully. Mount ended with status {}".format(
                    live_mount_info['status']))
        logger.warning("Live mount of the backup files completed.")
        return live_mount_info


class RubrikOracleBackupMountError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()

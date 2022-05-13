import rbs_oracle_common
import click
import logging
import sys
# import datetime
from datetime import datetime
import pytz
import operator


@click.command()
@click.option('--source_host_db', '-s', type=str, required=True,  help='The source <host or RAC cluster>:<database>')
@click.option('--sla', type=str, help='Pause the entire SLA')
@click.option('--inherit', '-h', type=str, help='Pause the entire SLA')
@click.option('--action', is_flag=False, help='Pause or UnPause the datebase backups')
@click.option('--wait', is_flag=True, help='Wait for backup to complete.')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING, ERROR or CRITICAL.')
def cli(source_host_db, sla, inherit, action, wait, debug_level):
    """
    This is pause or unpause an SLA or a Database

\b
    Returns:
        Status
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
    database = rbs_oracle_common.RubrikRbsOracleDatabase(rubrik, source_host_db[1], source_host_db[0])
    oracle_db_info = database.get_oracle_db_info()
    logger.warning(oracle_db_info['configuredSlaDomainId'])
    logger.warning(oracle_db_info['configuredSlaDomainName'])
    oracle_snapshot_info = database.get_oracle_db_snapshots()
    oracle_snapshots = []
    for snap in oracle_snapshot_info['data']:
        if not snap['isOnDemandSnapshot']:
            oracle_snapshots.append(snap)
    # sorted_oracle_snapshots = sorted(oracle_snapshots, key=lambda x: (datetime.strptime(x['date'].split('.')[0], '%Y-%m-%dT%H:%M:%S')))
    latest_snap = max(oracle_snapshots, key=lambda x: (datetime.strptime(x['date'].split('.')[0], '%Y-%m-%dT%H:%M:%S')))
    logger.debug("Latest Snapshot -> Backup Date: {}   Snapshot ID: {}".format(database.cluster_time(latest_snap['date'], rubrik.timezone)[:-6], latest_snap['id']))
    logger.warning("Last SLA Domain Policy applied: {0}".format(latest_snap['slaName']))
    exit()

    # Add block to protect or unprotect database

    logging.debug(oracle_log_backup_info)
    if wait:
        logger.warning("Starting archive log backup of database {} on {}".format(source_host_db[1], source_host_db[0]))
        oracle_log_backup_info = database.async_requests_wait(oracle_log_backup_info['id'], 12)
        logger.warning("Async request completed with status: {}".format(oracle_log_backup_info['status']))
        if oracle_log_backup_info['status'] != "SUCCEEDED":
            raise RubrikOracleManageProtectionError(
                "Database backup (snapshot) did not complete successfully. Mount ended with status {}".format(
                    oracle_log_backup_info['status']))
        logger.warning("Archive log backup completed.")
    else:
        cluster_timezone = pytz.timezone(rubrik.timezone)
        utc = pytz.utc
        start_time = utc.localize(datetime.datetime.fromisoformat(oracle_log_backup_info['startTime'][:-1])).astimezone(
            cluster_timezone)
        fmt = '%Y-%m-%d %H:%M:%S %Z'
        print("Oracle Log Backup {} \nStatus: {}, Started at {}.".format(oracle_log_backup_info['id'], oracle_log_backup_info['status'], start_time.strftime(fmt)))
    return oracle_log_backup_info


class RubrikOracleManageProtectionError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
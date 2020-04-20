import rbs_oracle_common
import click
import logging
import sys
import datetime
import pytz


@click.command()
@click.option('--source_host_db', '-s', type=str, required=True,  help='The source <host or RAC cluster>:<database>')
@click.option('--wait', is_flag=True, help='Wait for backup to complete.')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING, ERROR or CRITICAL.')
def cli(source_host_db, wait, debug_level):
    """
    This will initiate an on demand archive log backup of the database.

\b
    Returns:
        log_backup_info (dict): The information about the snapshot returned from the Rubrik CDM.
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
    oracle_log_backup_info = database.oracle_log_backup()
    logging.debug(oracle_log_backup_info)
    if wait:
        logger.warning("Starting archive log backup of database {} on {}".format(source_host_db[1], source_host_db[0]))
        oracle_log_backup_info = database.async_requests_wait(oracle_log_backup_info['id'], 12)
        logger.warning("Async request completed with status: {}".format(oracle_log_backup_info['status']))
        if oracle_log_backup_info['status'] != "SUCCEEDED":
            raise RubrikOracleLogBackupError(
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


class RubrikOracleLogBackupError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
import rbs_oracle_common
import click
import logging
import sys
import datetime
import pytz


@click.command()
@click.option('--source_host_db', '-s', type=str, required=True,  help='The source <host or RAC cluster>:<database>')
@click.option('--time_restore', '-t', type=str, help='Point in time to validate the DB, format is YY:MM:DDTHH:MM:SS example 2019-01-01T20:30:15')
@click.option('--host_target', '-h', type=str, help='Target Host for DB Validation ')
@click.option('--no_wait', is_flag=True, help='Queue DB Validate and exit.')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING, ERROR or CRITICAL.')
def cli(source_host_db, time_restore, host_target, no_wait, debug_level):
    """
    This will Validate the requested Rubrik Oracle backup set on source or target host or RAC cluster

\b
    The source database is specified in a host:db format.  If the restore time is not
    provided the most recent recoverable time will be used. The host for the validation can be specified if it is not it
    will be validate on the source host.
\b
    Returns:
        oracle_validate_info (dict): The information about the requested database validate returned from the Rubrik CDM.
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
    # The CDM version must be 5.3+ or there is no validate available
    cdm_version = rubrik.version.split("-")[0].split(".")
    if int(cdm_version[0]) < 6 and int(cdm_version[1]) < 3:
        logger.info("Cluster version {} is pre 5.3. Oracle Database validation is not available".format(cdm_version))
        raise RubrikOracleBackupValidateError(
            "Oracle Database Validation is not available in CDM version {}. Please upgrade to 5.3+ for this functionality".format(
                cdm_version))
    else:
        logger.debug("Cluster version {}.{}.{} is post 5.3".format(cdm_version[0], cdm_version[1], cdm_version[2]))

    source_host_db = source_host_db.split(":")
    database = rbs_oracle_common.RubrikRbsOracleDatabase(rubrik, source_host_db[1], source_host_db[0])
    oracle_db_info = database.get_oracle_db_info()
    logger.debug(oracle_db_info)
    if not host_target:
        host_target = source_host_db[0]
    target_id = database.get_target_id(rubrik.cluster_id, host_target)
    if time_restore:
        time_ms = database.epoch_time(time_restore, rubrik.timezone)
        logger.warning("Validating backup pieces for a point in time restore to time: {}.". format(time_restore))
    else:
        logger.warning("Using most recent recovery point for Validation.")
        time_ms = database.epoch_time(oracle_db_info['latestRecoveryPoint'], rubrik.timezone)

    logger.warning("Starting the Validation of the requested {} backup pieces on {}.".format(source_host_db[1], host_target))
    oracle_validate_info = database.oracle_validate(target_id, time_ms)
    cluster_timezone = pytz.timezone(rubrik.timezone)
    utc = pytz.utc
    start_time = utc.localize(datetime.datetime.fromisoformat(oracle_validate_info['startTime'][:-1])).astimezone(cluster_timezone)
    fmt = '%Y-%m-%d %H:%M:%S %Z'
    logger.info("Validate job requested at {}.".format(start_time.strftime(fmt)))
    logger.info("No wait flag is set to {}.".format(no_wait))
    if no_wait:
        logger.warning("Validate job id: {} Job status: {}.".format(oracle_validate_info['id'], oracle_validate_info['status']))
        return oracle_validate_info
    else:
        oracle_validate_info = database.async_requests_wait(oracle_validate_info['id'], 120)
        logger.warning("Async request completed with status: {}".format(oracle_validate_info['status']))
        if oracle_validate_info['status'] != "SUCCEEDED":
            raise RubrikOracleBackupValidateError(
                "Database validate did not complete successfully. Mount ended with status {}".format(
                    oracle_validate_info['status']))
        logger.warning("Database validate job completed.")
        return oracle_validate_info


class RubrikOracleBackupValidateError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()

import rbs_oracle_common
import click
import logging
import sys
import datetime
import pytz


@click.command()
@click.option('--source_host_db', '-s', type=str, required=True,  help='The source <host or RAC cluster>:<database>')
@click.option('--no_wait', is_flag=True, help='Queue database refresh and exit. This option is always set for now.')
@click.option('--keyfile', '-k', type=str, required=False,  help='The connection keyfile path')
@click.option('--insecure', is_flag=True,  help='Flag to use insecure connection')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING, ERROR or CRITICAL.')
def cli(source_host_db, no_wait, keyfile, insecure, debug_level):
    """
    This will initiate an on demand archive log backup of the database.

\b
    Returns:
        log_backup_info (dict): The information about the snapshot returned from the Rubrik CDM.
    """
    # set no_wait until response api is fixed
    no_wait = True
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

    rubrik = rbs_oracle_common.RubrikConnection(keyfile, insecure)
    source_host_db = source_host_db.split(":")
    logger.debug(source_host_db)
    if len(source_host_db) > 1:
        if source_host_db[1]:
            # refresh db check for v6+
            if (int(rubrik.version.split("-")[0].split(".")[0]) >= 7) or (int(rubrik.version.split("-")[0].split(".")[0]) == 6 and int(rubrik.version.split("-")[0].split(".")[2]) >= 2):
                logger.debug("Rubrik version is greater than 6.0.2, database refresh is supported.")
                logger.warning("Refreshing database: {0}".format(source_host_db[1]))
                database = rbs_oracle_common.RubrikRbsOracleDatabase(rubrik, source_host_db[1], source_host_db[0])
                refresh_response = database.refresh()
                response_url = refresh_response['links'][0]['href']
                logger.debug(refresh_response)
                # Set the time format for the printed result
                cluster_timezone = pytz.timezone(rubrik.timezone)
                utc = pytz.utc
                start_time = utc.localize(
                    datetime.datetime.fromisoformat(refresh_response['startTime'][:-1])).astimezone(cluster_timezone)
                fmt = '%Y-%m-%d %H:%M:%S %Z'
                logger.debug("Host refresh status: {0}, Started at {1}.".format(refresh_response['status'], start_time.strftime(fmt)))
                if no_wait:
                    cluster_timezone = pytz.timezone(rubrik.timezone)
                    utc = pytz.utc
                    start_time = utc.localize(
                        datetime.datetime.fromisoformat(refresh_response['startTime'][:-1])).astimezone(
                        cluster_timezone)
                    fmt = '%Y-%m-%d %H:%M:%S %Z'
                    logger.warning("Oracle Database {0} refresh started at {1}, Status: {2} ".format(source_host_db[1], start_time.strftime(fmt), refresh_response['status']))
                else:
                    logger.warning(
                        "Starting refresh of database {} on {}".format(source_host_db[1], source_host_db[0]))
                    refresh_response = database.async_requests_wait(refresh_response['id'], 12)
                    logger.warning("Database refresh in progress with status: {}".format(refresh_response['status']))
                    if refresh_response['status'] != "SUCCEEDED":
                        raise RubrikOracleRBSRefreshError(
                            "Database refresh did not complete successfully. Job ended with status {}".format(
                                refresh_response['status']))
                    logger.warning("Oracle database refresh completed.")
            else:
                logger.warning("CDM version is pre v6. Database refresh is not supported. Refreshing host: {0}".format(source_host_db[0]))
                host = rbs_oracle_common.RubrikRbsOracleHost(rubrik, source_host_db[0])
                logger.warning("Refreshing host: {0}, with host ID: {1}".format(source_host_db[0], host.id))
                refresh_response = host.refresh()
                logger.debug("Host Refresh complete: {0}".format(refresh_response))
                logger.warning("Host Refresh complete!")
        else:
            # refresh host
            host = rbs_oracle_common.RubrikRbsOracleHost(rubrik, source_host_db[0])
            logger.warning("Refreshing host: {0}".format(source_host_db[0]))
            logger.warning("Refresh in progress...")
            refresh_response = host.refresh()
            logger.debug("Host Refresh complete: {0}".format(refresh_response))
            logger.warning("Host Refresh complete!")
    else:
        # refresh host
        host = rbs_oracle_common.RubrikRbsOracleHost(rubrik, source_host_db[0])
        logger.warning("Refreshing host: {0}".format(source_host_db[0]))
        logger.warning("Refresh in progress...")
        refresh_response = host.refresh()
        logger.debug("Host Refresh complete: {0}".format(refresh_response))
        logger.warning("Host Refresh complete!")
    rubrik.delete_session()
    return refresh_response


class RubrikOracleRBSRefreshError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
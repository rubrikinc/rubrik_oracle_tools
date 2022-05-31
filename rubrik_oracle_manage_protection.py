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
@click.option('--inherit', '-i', is_flag=True, help='Inherit the SLA from the parent object')
@click.option('--action', '-a', required=True, type=click.Choice(['pause', 'resume'], case_sensitive=False))
@click.option('--wait', is_flag=True, help='Wait for backup to complete.')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING, ERROR or CRITICAL.')
def cli(source_host_db, inherit, action, wait, debug_level):
    """    This will pause or resume database backups by managing the protection.

\b
Pause will stop the database badkups (both database and archive log) by setting the database to Unprotected.
Resume will restore the last SLA Domain Policy applied (at the last snapshot). If the SLA Domain Policy is
 iherited from the parent object (host/cluster) that can be set using the inherit script parameter (-i) and the
 database will be set to derive it's protection from it's parent. The API Token user must have permissions on the SLA
 Domain Policy to be used. This will only work for Rubrik CDM 7.0 and above.
\b
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

    logger.warning("Database is currently protected with SLA Domain Policy: {0}".format(oracle_db_info['configuredSlaDomainName']))
    if action == 'pause':
        logger.warning("Setting database to unprotected.")
        unprotect_result = database.oracle_db_unprotect()
        logger.warning("Pending SLA Domain Name: {0}".format(unprotect_result[0]['pendingSlaDomainName']))
        if wait:
            logger.warning("Waiting will while the database is set to {0}".format(unprotect_result[0]['pendingSlaDomainName']))
            oracle_db_info = database.async_sla_change_wait(unprotect_result[0]['pendingSlaDomainName'], 15)
            logger.debug("DB info after wait: {0}".format(oracle_db_info))
            logger.warning("The database is now set to {0}".format(oracle_db_info['effectiveSlaDomainName']))
    elif action == 'resume':
        oracle_snapshot_info = database.get_oracle_db_snapshots()
        oracle_snapshots = []
        for snap in oracle_snapshot_info['data']:
            if not snap['isOnDemandSnapshot']:
                oracle_snapshots.append(snap)
        latest_snap = max(oracle_snapshots,
                          key=lambda x: (datetime.strptime(x['date'].split('.')[0], '%Y-%m-%dT%H:%M:%S')))
        logger.debug("Latest Snapshot -> Backup Date: {}   Snapshot ID: {}".format(database.cluster_time(latest_snap['date'], rubrik.timezone)[:-6], latest_snap['id']))
        if inherit:
            protect_result = database.oracle_db_protect('', inherit)
            logger.warning("Database updated to inherit SLA Domain from host/cluster.")
            if wait:
                logger.warning("Waiting will while the database is set to inherit it's SLA Domain")
                oracle_db_info = database.async_sla_change_wait('inherit', 15)
                logger.debug("DB info after wait: {0}".format(oracle_db_info))
                logger.warning("The database is now set to {0}".format(oracle_db_info['effectiveSlaDomainName']))
            else:
                logger.warning("Pending SLA Domain Name: {0}".format(protect_result[0]['pendingSlaDomainName']))
        else:
            logger.warning("Last SLA Domain Policy applied: {0}   ID: {1}".format(latest_snap['slaName'], latest_snap['slaId']))
            protect_result = database.oracle_db_protect(latest_snap['slaId'])
            logger.debug("Protect result: {0}".format(protect_result))
            if wait:
                logger.warning("Waiting will while the database is set to {0}".format(latest_snap['slaName']))
                oracle_db_info = database.async_sla_change_wait(latest_snap['slaName'], 15)
                logger.warning("New SLA: {0}".format(latest_snap['slaName']))
                logger.debug("DB info after wait: {0}".format(oracle_db_info))
                logger.warning("The database is now set to {0}".format(oracle_db_info['effectiveSlaDomainName']))
            else:
                logger.warning("Pending SLA: {0}".format(latest_snap['slaName']))
    else:
        logger.waring("No action [pause, resume] was supplied")
        raise RubrikOracleManageProtectionError("No action [pause, resume] was supplied")

    return


class RubrikOracleManageProtectionError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
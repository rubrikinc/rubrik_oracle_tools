import click
import logging
import sys
from tabulate import tabulate
import rbs_oracle_common


@click.command()
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING or CRITICAL.')
def cli(debug_level):
    """
    Displays information about the Oracle database object, the available snapshots, and recovery ranges.
    If no source_host_db is supplied, all non-relic Oracle databases will be listed.
    Recommended console line size is 120 characters.
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
    t = rbs_oracle_common.Timer(text="RBS Connection took {:0.2f} seconds", logger=logging.debug)
    t.start()
    rubrik = rbs_oracle_common.RubrikConnection()
    t.stop()
    print("*" * 110)
    print("Connected to cluster: {}, version: {}, Timezone: {}.".format(rubrik.name, rubrik.version, rubrik.timezone))
    t = rbs_oracle_common.Timer(text="Database list GET took {:0.2f} seconds", logger=logging.debug)
    t.start()
    databases = rubrik.connection.get("internal", "/oracle/db")
    t.stop()
    db_data = []
    db_headers = ["Host/Cluster", "Database", "DG_Group", "SLA", "Log Freq", "Last DB BKUP", "Last LOG BKUP", "Missed"]
    for db in databases['data']:
        db_element = [''] * 8
        dg_id = False
        if not db['isRelic']:
            if 'standaloneHostName' in db.keys():
                db_element[0] = db['standaloneHostName']
            elif 'racName' in db.keys():
                db_element[0] = db['racName']
            db_element[1] = db['sid']
            if 'dataGuardType' in db.keys():
                if db['dataGuardType'] == 'DataGuardMember':
                    db_element[2] = db['dataGuardGroupName'].split('DG_GROUP_')[1]
                    dg_id = db['dataGuardGroupId']
                    dg_groups = rubrik.connection.get("v1", "/oracle/db/{0}".format(db['dataGuardGroupId']))
                    db_element[3] = dg_groups['effectiveSlaDomainName']
                else:
                    db_element[2] = 'None'
                    db_element[3] = db['effectiveSlaDomainName']
            if 'logBackupFrequencyInMinutes' in db.keys():
                db_element[4] = db['logBackupFrequencyInMinutes']
            else:
                db_element[4] = "None"
            if 'lastSnapshotTime' in db.keys():
                db_element[5] = db['lastSnapshotTime'][:-5]
            else:
                db_element[5] = "None"
            db_element[7] = db['numMissedSnapshot']
            # t = rbs_oracle_common.Timer(text="Database details GET took {:0.2f} seconds", logger=logging.debug)
            # t.start()
            # database = rbs_oracle_common.RubrikRbsOracleDatabase(rubrik, db_element[1], db_element[0])
            # oracle_db_info1 = database.get_oracle_db_info()
            # t.stop()
            if dg_id:
                id = dg_id
            else:
                id = db['id']
            t = rbs_oracle_common.Timer(text="Database details direct GET took {:0.2f} seconds", logger=logging.debug)
            t.start()
            oracle_db_details = rubrik.connection.get("v1", "/oracle/db/{0}".format(id))
            t.stop()
            if 'latestRecoveryPoint' in oracle_db_details.keys():
                # db_element[6] = format(rbs_oracle_common.cluster_time(oracle_db_details['latestRecoveryPoint'], rubrik.timezone)[:-6])
                db_element[6] = oracle_db_details['latestRecoveryPoint']
            else:
                db_element[6] = "None"
            db_data.append(db_element)
    db_data.sort(key=lambda x: (x[0], x[1]))
    print("*" * 110)
    print(tabulate(db_data, headers=db_headers))


class RubrikOracleBackupInfoError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()

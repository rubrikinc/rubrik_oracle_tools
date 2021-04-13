import click
import logging
import sys
from tabulate import tabulate
import rbs_oracle_common


@click.command()
@click.option('--source_host_db', '-s', type=str, required=False,  help='The source <host or RAC cluster>:<database>')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING or CRITICAL.')
def cli(source_host_db, debug_level):
    """
    Displays information about the Oracle database object, the available snapshots, and recovery ranges.
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
    print("*" * 100)
    print("Connected to cluster: {}, version: {}, Timezone: {}.".format(rubrik.name, rubrik.version, rubrik.timezone))
    if source_host_db:
        source_host_db = source_host_db.split(":")
        database = rbs_oracle_common.RubrikRbsOracleDatabase(rubrik, source_host_db[1], source_host_db[0])
        oracle_db_info = database.get_oracle_db_info()
        print("*" * 100)
        print("Database Details: ")
        print("Database name: {}   ID: {}".format(oracle_db_info['name'], oracle_db_info['id']))
        if 'standaloneHostName' in oracle_db_info.keys():
            print("Host Name: {}".format(oracle_db_info['standaloneHostName']))
        elif 'racName' in oracle_db_info.keys():
            print("Rac Cluster Name: {}    Instances: {}".format(oracle_db_info['racName'], oracle_db_info['numInstances']))
        print("SLA: {}    Log Backup Frequency: {} min.    Log Retention: {} hrs.".format(oracle_db_info['effectiveSlaDomainName'], oracle_db_info['logBackupFrequencyInMinutes'], oracle_db_info['logRetentionHours']))
        oracle_snapshot_info = database.get_oracle_db_snapshots()
        logger.debug(oracle_snapshot_info)
        print("*" * 100)
        print("Available Database Backups (Snapshots):")
        for snap in oracle_snapshot_info['data']:
            print("Database Backup Date: {}   Snapshot ID: {}".format(database.cluster_time(snap['date'], rubrik.timezone)[:-6], snap['id']))

        oracle_db_recoverable_range_info = database.get_oracle_db_recoverable_range()
        print("*" * 100)
        print("Recoverable ranges:")
        for recovery_range in oracle_db_recoverable_range_info['data']:
            print("Begin Time: {}   End Time: {}".format(database.cluster_time(recovery_range['beginTime'], rubrik.timezone)[:-6],
                                                         database.cluster_time(recovery_range['endTime'], rubrik.timezone)[:-6]))
    else:
        databases = rubrik.connection.get("internal", "/oracle/db")
        db_data = []
        db_headers = ["Host/Cluster", "Database", "SLA", "Log Freq", "Last DB BKUP", "Missed"]
        for db in databases['data']:
            db_element = [''] * 6
            if not db['isRelic']:
                if 'standaloneHostName' in db.keys():
                    # db_element.append(db['standaloneHostName'])
                    db_element[0] = db['standaloneHostName']
                elif 'racName' in db.keys():
                    db_element[0] = db['racName']
                db_element[1] = db['sid']
                db_element[2] = db['effectiveSlaDomainName']
                if 'logBackupFrequencyInMinutes' in db.keys():
                    db_element[3] = db['logBackupFrequencyInMinutes']
                else:
                    db_element[3] = "None"
                if 'lastSnapshotTime' in db.keys():
                    db_element[4] = db['lastSnapshotTime'][:-5]
                else:
                    db_element[4] = "None"
                db_element[5] = db['numMissedSnapshot']
                db_data.append(db_element)
        db_data.sort(key = lambda x: x[0])
        print("*" * 100)
        print(tabulate(db_data,headers=db_headers))


class RubrikOracleBackupInfoError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()

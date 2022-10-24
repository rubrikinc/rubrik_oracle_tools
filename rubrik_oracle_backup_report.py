import click
import logging
import sys
from tabulate import tabulate
import rbs_oracle_common
import concurrent.futures

@click.command()
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING or CRITICAL.')
def cli(debug_level):
    """
    Displays information about all non-relic Oracle databases.
    Recommended console line size is 180 characters.
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

    overall_timer = rbs_oracle_common.Timer(text="Database report completed in {:0.2f} seconds", logger=logging.warning)
    overall_timer.start()
    t = rbs_oracle_common.Timer(text="RBS Connection took {:0.2f} seconds", logger=logging.debug)
    t.start()
    global rubrik
    rubrik = rbs_oracle_common.RubrikConnection()
    t.stop()
    print("*" * 110)
    print("Connected to cluster: {}, version: {}, Timezone: {}.".format(rubrik.name, rubrik.version, rubrik.timezone))
    t = rbs_oracle_common.Timer(text="Database list GET took {:0.2f} seconds", logger=logging.debug)
    t.start()
    databases = rubrik.connection.get("internal", "/oracle/db")
    t.stop()
    db_data = []
    dg_group_ids = []
    db_headers = ["Host/Cluster", "Database", "DG_Group", "SLA", "Log Freq", "Last DB BKUP", "Last LOG BKUP", "Missed"]
    db_data = []
    db_list = []
    for db in databases['data']:
        if not db['isRelic'] and db['dataGuardType'] == 'NonDataGuard':
            db_list.append(db['id'])
        elif not db['isRelic'] and db['dataGuardType'] == 'DataGuardMember':
            db_list.append(db['dataGuardGroupId'])
    db_list = list(set(db_list))
    logger.debug("Thread list: {}".format(db_list))
    global element_list
    element_list = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        executor.map(get_db_data, db_list)

    logger.debug("Get_db_data return: {}".format(element_list))
    element_list.sort(key=lambda x: (x[0], x[1]))
    print("*" * 110)
    print(tabulate(element_list, headers=db_headers))
    print('\r\r\r')
    overall_timer.stop()

def get_db_data(id):
    db_element = [''] * 8
    t = rbs_oracle_common.Timer(text="Database details direct GET took {:0.2f} seconds", logger=logging.debug)
    t.start()
    oracle_db_details = rubrik.connection.get("v1", "/oracle/db/{0}".format(id))
    logging.debug("Oracle db details: {}".format(oracle_db_details))
    if oracle_db_details['dataGuardType'] == 'DataGuardGroup':
        logging.debug("DG Group: {}".format(oracle_db_details['dbUniqueName']))
        for member in oracle_db_details['dataGuardGroupMembers']:
            logging.debug("DG_GROUP member: {}".format(member['dbUniqueName']))
            db_element = [''] * 8
            if 'standaloneHostName' in member.keys():
                db_element[0] = member['standaloneHostName']
            elif 'racName' in member.keys():
                db_element[0] = member['racName']
            db_element[1] = member['dbUniqueName'] + '-' + member['role']
            db_element[2] = oracle_db_details['dbUniqueName']
            db_element[3] = oracle_db_details['effectiveSlaDomainName']
            if 'logBackupFrequencyInMinutes' in oracle_db_details.keys():
                db_element[4] = oracle_db_details['logBackupFrequencyInMinutes']
            else:
                db_element[4] = "None"
            if 'lastSnapshotTime' in oracle_db_details.keys():
                db_element[5] = oracle_db_details['lastSnapshotTime'][:-5]
            else:
                db_element[5] = "None"
            db_element[6] = oracle_db_details['latestRecoveryPoint']
            if 'latestRecoveryPoint' in oracle_db_details.keys():
                db_element[6] = oracle_db_details['latestRecoveryPoint']
                db_element[6] = format(
                    rbs_oracle_common.RubrikRbsOracleDatabase.cluster_time(oracle_db_details['latestRecoveryPoint'],
                                                                           rubrik.timezone)[:-6])
            else:
                db_element[6] = "None"
            db_element[7] = oracle_db_details['numMissedSnapshot']
            element_list.append(db_element)
    elif oracle_db_details['dataGuardType'] == 'NonDataGuard':
        if 'standaloneHostName' in oracle_db_details.keys():
            db_element[0] = oracle_db_details['standaloneHostName']
        elif 'racName' in oracle_db_details.keys():
            db_element[0] = oracle_db_details['racName']
        db_element[1] = oracle_db_details['sid']
        db_element[2] = 'None'
        db_element[3] = oracle_db_details['effectiveSlaDomainName']
        if 'logBackupFrequencyInMinutes' in oracle_db_details.keys():
            db_element[4] = oracle_db_details['logBackupFrequencyInMinutes']
        else:
            db_element[4] = "None"
        if 'lastSnapshotTime' in oracle_db_details.keys():
            db_element[5] = oracle_db_details['lastSnapshotTime'][:-5]
        else:
            db_element[5] = "None"
        if 'latestRecoveryPoint' in oracle_db_details.keys():
            db_element[6] = format(
                rbs_oracle_common.RubrikRbsOracleDatabase.cluster_time(oracle_db_details['latestRecoveryPoint'],
                                                                       rubrik.timezone)[:-6])
        else:
            db_element[6] = "None"
        db_element[7] = oracle_db_details['numMissedSnapshot']
        element_list.append(db_element)
    return


class RubrikOracleBackupInfoError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()

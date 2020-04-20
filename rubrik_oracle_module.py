#!/usr/bin/env python3
"""Module of functions for Rubrik Oracle
"""
import rubrik_cdm
import logging
import datetime
import pytz
import json
import os
import subprocess
from subprocess import PIPE, Popen
import sys
import inspect
import re
import time
import glob
from yaspin import yaspin
import urllib3

urllib3.disable_warnings()
logger = logging.getLogger(__name__)


class NoTraceBackWithLineNumber(Exception):
    """
    Limits Traceback on raise and only will raise object name and line number
    """
    def __init__(self, msg):
        try:
            ln = sys.exc_info()[-1].tb_lineno
        except AttributeError:
            ln = inspect.currentframe().f_back.f_lineno
        self.args = "{0.__name__} (line {1}): {2}".format(type(self), ln, msg),
        sys.exit(self)


class RubrikOracleModuleError(NoTraceBackWithLineNumber):
    """
    Renames object so error is named with calling script
    """
    pass


def connect_rubrik():
    """
    Creates a connection to the Rubrik CDM.

    Returns:
        rubrik_connection_object: A connection to the Rubrik CDM
    """
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    with open(os.path.join(__location__, 'config.json')) as config_file:
        config = json.load(config_file)
    if not config['rubrik_cdm_node_ip']:
        config['rubrik_cdm_node_ip'] = None
    return rubrik_cdm.Connect(config['rubrik_cdm_node_ip'], config['rubrik_cdm_username'], config['rubrik_cdm_password'], config['rubrik_cdm_token'])


def get_oracle_db_id(rubrik, oracle_db_name, oracle_host_name):
    """
    Get the Oracle object id from the Rubrik CDM using database name and the hostname.

    This is just a wrapper on object_id function in the Rubrik CDM module.

    Args:
        rubrik (class): Rubrik CDM connection object
        oracle_db_name (str): The database name.
        oracle_host_name (str):  The host name or cluster name that the db is running on.

    Returns:
        oracle_db_id (str): The Rubrik database object id.
    """

    # This will use the rubrik_cdm module to get the id. There is a bug that is getting fixed so until
    # that fix is in place get the id using a basic Get.
    #     oracle_db_id = rubrik.object_id(oracle_db_name, 'oracle_db', hostname=oracle_host_name)
    #     return oracle_db_id

    oracle_dbs = rubrik.get("internal", "/oracle/db?name={}".format(oracle_db_name), timeout=60)
    logger.debug("Return from /oracle/db?name={}:".format(oracle_db_name))
    logger.debug(oracle_dbs)
    oracle_id = ''
    # Find the oracle_db object with the correct hostName or RAC cluster name.
    # Instance names can be stored/entered with and without the domain name so
    # we will compare the hostname without the domain.
    logger.debug("Searching for the hostname match in the list of returned databases")
    if is_ip(oracle_host_name):
        raise RubrikOracleModuleError("A hostname is required for the Oracle host, do not use an IP address.")
    oracle_host_name = oracle_host_name.split('.')[0]
    if oracle_dbs['total'] == 0:
        raise RubrikOracleModuleError("The database object '{}' was not found on the Rubrik cluster.".format(oracle_db_name))
    elif oracle_dbs['total'] > 0:
        for db in oracle_dbs['data']:
            if 'standaloneHostName' in db.keys():
                if oracle_host_name == db['standaloneHostName'].split('.')[0]:
                    oracle_id = db['id']
                    break
            elif 'racName' in db.keys():
                if oracle_host_name == db['racName']:
                    oracle_id = db['id']
                    break
                if any(instance['hostName'] == oracle_host_name for instance in  db['instances']):
                    oracle_id = db['id']
                    break
    if not oracle_id:
        raise RubrikOracleModuleError("The database '{}' on the host '{}' was not found on the Rubrik cluster.".format(oracle_db_name, oracle_host_name))
    return oracle_id


def get_oracle_db_info(rubrik, oracle_db_id):
    """
    Gets the information about a Rubrik Oracle database object using the Rubrik Oracle database id.

    Args:
        rubrik (class):  Rubrik CDM connection object.
        oracle_db_id (str): The Rubrik database object id.

    Returns:
        oracle_db_info (dict): The json returned  from the Rubrik CDM with the database information converted to a dictionary.
    """
    oracle_db_info = rubrik.get('internal', '/oracle/db/{}'.format(oracle_db_id), timeout=60)
    return oracle_db_info


def get_oracle_db_recoverable_range(rubrik, oracle_db_id):
    """
        Gets the Rubrik Oracle database object's available recovery ranges using the Rubrik Oracle database id.

        Args:
            rubrik (class):  Rubrik CDM connection object.
            oracle_db_id (str): The Rubrik database object id.

        Returns:
            oracle_db_recoverable_range_info (dict): The Rubrik CDM database recovery ranges.
    """
    oracle_db_recoverable_range_info = rubrik.get('internal', '/oracle/db/{}/recoverable_range'.format(oracle_db_id), timeout=60)
    return oracle_db_recoverable_range_info


def get_oracle_db_snapshots(rubrik, oracle_db_id):
    """
        Gets the Rubrik Oracle database object's available snapshots using the Rubrik Oracle database id.

        Args:
            rubrik (class):  Rubrik CDM connection object.
            oracle_db_id (str): The Rubrik database object id.

        Returns:
            oracle_db_recoverable_range_info (dict): The Rubrik CDM database available snapshots.
    """
    oracle_db_snapshot_info = rubrik.get('internal', '/oracle/db/{}/snapshot'.format(oracle_db_id), timeout=60)
    return oracle_db_snapshot_info


def epoch_time(time_string, timezone):
    """
    Converts a time string in ISO 8601 format to epoch time using the time zone.

    Args:
        time_string (str): A time string in ISO 8601 format. If the string ends with Z it is considered to be in ZULU (GMT)
        timezone (str): The timezone.

    Returns:
        epoch_time (str): the epoch time.
    """
    if time_string.endswith('Z'):
        time_string = time_string[:-1]
        utc = pytz.utc
        datetime_object = utc.localize(datetime.datetime.fromisoformat(time_string))
    else:
        cluster_timezone = pytz.timezone(timezone)
        datetime_object = cluster_timezone.localize(datetime.datetime.fromisoformat(time_string))
    return int(datetime_object.timestamp()) * 1000


def cluster_time(time_string, timezone):
    """
    Converts a time string in a timezone to a user friendly string in that time zone.

\b
    Args:
        time_string (str): Time string.
        timezone (str): Time zone.
\b
    Returns:
        time_string (str): Time string converted to the supplied time zone.
    """
    cluster_timezone = pytz.timezone(timezone)
    utc = pytz.utc
    if time_string.endswith('Z'):
        time_string = time_string[:-1]
        datetime_object = utc.localize(datetime.datetime.fromisoformat(time_string))
    else:
        datetime_object = cluster_timezone.localize(datetime.datetime.fromisoformat(time_string))
    cluster_time_object = cluster_timezone.normalize(datetime_object.astimezone(cluster_timezone))
    return cluster_time_object.isoformat()


def get_cluster_info(rubrik):
    """
    Returns basic information about the Rubrik CDM cluster.

    Args:
        rubrik:   Rubrik CDM connection object.

    Returns:
        cluster_info (dict): The information about the cluster returned from the Rubrik CDM.
    """
    cluster_info = rubrik.get('v1','/cluster/me', timeout=60)
    return cluster_info


def live_mount(rubrik, oracle_db_id, host_id, time_ms, files_only=False, mount_path=None):
    """
    Live mounts a Rubrik Database backup on the requested host or cluster.

    Args:
        rubrik (class):   Rubrik CDM connection object.
        oracle_db_id (str): The Rubrik Oracle database id.
        host_id (str):  The Rubrik host or cluster for the mount.
        time_ms  (str):  The point in time of the backup to mount.
        files_only (bool):  Mount the backup pieces only.
        mount_path (str):  The path to mount the files only restore. (Required if files_only is True).

    Returns:
        live_mount_info (dict): The information about the requested live mount returned from the Rubrik CDM.
    """
    payload = {
        "recoveryPoint": {"timestampMs": time_ms},
        "targetOracleHostOrRacId": host_id,
        "targetMountPath": mount_path,
        "shouldMountFilesOnly": files_only
    }
    live_mount_info = rubrik.post('internal', '/oracle/db/{}/mount'.format(oracle_db_id), payload, timeout=60)
    return live_mount_info


def live_mount_delete(rubrik, live_mount_id, force):
    """
    This will unmount a live mounted database or backup set.

    Args:
        rubrik (class): A rubrik_connection_object.
        live_mount_id (str): The id of the mount to remove,
        force (bool): Set to true to force the unmount.

    Returns:
        live_mount_delete_info (dict): The information returned from the Rubrik CDM about the requested unmount.
    """
    live_mount_delete_info = rubrik.delete('internal', '/oracle/db/mount/{}?force={}'.format(live_mount_id, force))
    return live_mount_delete_info


def get_host_id(rubrik, primary_cluster_id, hostname):
    """
    Gets the Oracle database host using the hostname.

    Args:
        rubrik (class): A rubrik_connection_object
        hostname (str): The oracle host name
        primary_cluster_id (str): The rubrik cluster id

    Returns:
        host_id (str): The host id
    """
    host_info = rubrik.get('internal', '/oracle/host?name={}'.format(hostname))
    host_id = ''
    if host_info['total'] == 0:
        raise RubrikOracleModuleError("The host: {} was not found on the Rubrik CDM.".format(hostname))
    elif host_info['total'] > 1:
        # found_hosts = []
        for hosts in host_info['data']:
            if hosts['primaryClusterId'] == primary_cluster_id and hosts['status'] == 'Connected':
                host_id = hosts['id']
        # raise RubrikOracleModuleError("Multiple Host IDs found: {} ".format(found_hosts))
    else:
        host_id = host_info['data'][0]['id']
    return host_id


def get_rac_id(rubrik, primary_cluster_id, rac_cluster_name):
    """
    Gets the RAC Cluster ID using the cluster name.

    Args:
        rubrik (class): A rubrik_connection_object.
        rac_cluster_name (str): The RAC cluster name.
        primary_cluster_id (str): The rubrik cluster id

    Returns:
        rac_id (str): The RAC Cluster ID  if found otherwise will exit with error condition.
    """
    rac_info = rubrik.get('internal', '/oracle/rac?name={}'.format(rac_cluster_name))
    rac_id = ''
    if rac_info['total'] == 0:
        raise RubrikOracleModuleError("The target: {} either was not found or is not a RAC cluster.".format(rac_cluster_name))
    elif rac_info['total'] > 1:
        for rac in rac_info['data']:
            if rac['primaryClusterId'] == primary_cluster_id and rac['status'] == 'Connected':
                rac_id = rac['id']
                break
        # raise RubrikOracleModuleError("Multiple RAC IDs found: {} ".format(found_clusters))
    else:
        rac_id = rac_info['data'][0]['id']
    return rac_id


def get_oracle_live_mount_id(rubrik, primary_cluster_id, db_name, host_cluster):
    """
    This will search for and retrieve the live mount id for a live mount of the database on the host.

    Args:
        rubrik (class): A rubrik_connection_object.
        db_name (str): The database name.
        host_cluster (str): The host or cluster name. If the live mount is on a cluster this can be the cluster name
        or the host name of one of the nodes in the cluster
        primary_cluster_id (str): The rubrik cluster id

    Returns:
        live_mount_id (str): The id of the requested live mount.
    """
    logger.debug("get_oracle_live_mount_id(rubrik, {}, {}, {}".format(primary_cluster_id, db_name, host_cluster))
    oracle_live_mounts = rubrik.get('internal', '/oracle/db/mount?source_database_name={}'.format(db_name))
    logger.debug("Oracle live mounts: {}".format(oracle_live_mounts))
    live_mount_id = []
    # On CDM 5.1.1+ the targetHostID id is the host or cluster name first.
    logger.debug("Finding host or cluster '{}' match in Oracle live mounts.".format(host_cluster))
    for mount in oracle_live_mounts['data']:
        if host_cluster in mount['targetHostId']:
            live_mount_id.append(mount['id'])
    if live_mount_id:
        return live_mount_id
    # If no match the CDM release is pre 5.1.1 and we much find the id for the target host
    # Check if host_cluster is a RAC Cluster or a node in a RAC cluster so we can use the RAC cluster id
    rac_id = rubrik.get('internal', '/oracle/rac?name={}'.format(host_cluster))
    logger.debug("RAC objects with the cluster name {}".format(host_cluster))
    mount_host_id = ''
    if rac_id['total'] == 0:
        rac_info = rubrik.get('internal', '/oracle/rac')
        for rac in rac_info['data']:
            for nodes in rac['nodes']:
                if nodes['nodeName'] == host_cluster:
                    mount_host_id = rac['id']
            if mount_host_id:
                break
    elif rac_id['total'] == 1:
        mount_host_id = rac_id['data'][0]['id']
    else:
        for rac in rac_id['data']:
            if rac['primaryClusterId'] == primary_cluster_id and rac['status'] == 'Connected':
                mount_host_id = rac['id']
    if not mount_host_id:
        mount_host_id = get_host_id(rubrik, primary_cluster_id, host_cluster)
        logger.debug("Host id found is {}".format(mount_host_id))
    host_id = mount_host_id .split(':::')[1]
    for mount in oracle_live_mounts['data']:
        if host_id == mount['targetHostId']:
            live_mount_id.append(mount['id'])
    return live_mount_id


def get_sla_id(rubrik, sla_name):
    """
    Gets the Rubrik SLA ID for the SLA.

    Args:
        rubrik (class): A rubrik_connection_object.
        sla_name (str): The Rubrik SLA Domain name

    Returns:
        sla_id (str): The Rubrik SLA ID

    """
    sla_info = rubrik.get('v1', '/sla_domain?name={}'.format(sla_name))
    if sla_info['total'] == 0:
        raise RubrikOracleModuleError("The sla: {} was not found on this Rubrik cluster.".format(sla_name))
    elif sla_info['total'] > 1:
        raise RubrikOracleModuleError("Multiple SLAs with the name {} were found on this cluster.".format(sla_name))
    else:
        sla_id = sla_info['data'][0]['id']
    return sla_id


def oracle_db_snapshot(rubrik, db_id, sla_id, force):
    """
    Initiates an on demand snapshot of an Oracle database. Uses the current
    SLA for that database if an sla name is not supplied.  Forces a full backup if
    force is true.

    Args:
        rubrik (class): A rubrik_connection_object.
        db_id (str): The id for the database to be backed up.
        sla_id (str): The Rubrik SLA ID.
        force (bool): Force a full backup.

    Returns:
        A list of the information returned from the Rubrik CDM  from the Snapshot request.

    """
    payload = {
        "slaId": sla_id,
        "forceFullSnapshot": force
    }
    db_snapshot_info = rubrik.post('internal', '/oracle/db/{}/snapshot'.format(db_id), payload)
    return db_snapshot_info


def oracle_log_backup(rubrik, db_id):
    """
    Initiates an archive log backup of an Oracle database.

    Args:
        rubrik: A rubrik_connection_object.
        db_id (str): The id for the database to be backed up.

    Returns:
        A list of the information returned from the Rubrik CDM  from the log backup request.

    """

    oracle_log_backup_info = rubrik.post('internal', '/oracle/db/{}/log_backup'.format(db_id), '')
    return oracle_log_backup_info


def is_ip(hostname):
    """
    Checks if a hostname is an IP address.

    Args:
        hostname (str): The hostname to test to see if it's an IP address.

    Returns:
        True if hostname is an IP Address, False if it is not.
    """
    regex = '''^(25[0-5]|2[0-4][0-9]|[0-1]?[0-9][0-9]?)\.( 
                    25[0-5]|2[0-4][0-9]|[0-1]?[0-9][0-9]?)\.( 
                    25[0-5]|2[0-4][0-9]|[0-1]?[0-9][0-9]?)\.( 
                    25[0-5]|2[0-4][0-9]|[0-1]?[0-9][0-9]?)'''
    if re.search(regex, hostname):
        return True
    else:
        return False


def async_requests_wait(rubrik, requests_id, timeout):
    timeout_start = time.time()
    terminal_states = ['FAILED', 'CANCELED', 'SUCCEEDED']
    while time.time() < timeout_start + (timeout * 60):
        oracle_request = rubrik.get('internal', '/oracle/request/{}'.format(requests_id), timeout=60)
        if oracle_request['status'] in terminal_states:
            break
        with yaspin(text='Request status: {}'.format(oracle_request['status'])):
            time.sleep(10)
    if oracle_request['status'] not in terminal_states:
        raise RubrikOracleScriptTimeoutError(
            "\nTimeout: Async request status has been {0} for longer than the timeout period of {1} minutes. The request will remain active (current status: {0})  and the script will exit.".format(
                oracle_request['status'], timeout))
    else:
        return oracle_request


def oracle_db_rename(oracle_sid, oracle_home, new_oracle_name):
    os.environ["ORACLE_HOME"] = oracle_home
    os.environ["ORACLE_SID"] = oracle_sid
    logger.info(sqlplus_sysdba(oracle_home, 'shutdown immediate'))
    logger.info(sqlplus_sysdba(oracle_home, 'startup mount'))
    # Change the database name
    logfile = oracle_home + '/dbs/nid_' + new_oracle_name + '.log'
    logger.info("NID Logfile: {}".format(logfile))
    session = Popen(['nid', 'target=/', 'dbname={}'.format(new_oracle_name), 'logfile={}'.format(logfile)], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    stdout, stderr = session.communicate()
    logger.info(stdout.decode())
    # Create an init file from the spfile
    logger.info(sqlplus_sysdba(oracle_home, 'create pfile from spfile;'))
    # Rename the init file
    os.rename("{}/dbs/init{}.ora".format(oracle_home, oracle_sid), "{}/dbs/init{}.ora".format(oracle_home, new_oracle_name))
    # Rename the password file if present
    if os.path.exists("{}/dbs/orapw{}".format(oracle_home, oracle_sid)):
        os.rename("{}/dbs/orapw{}".format(oracle_home, oracle_sid), "{}/dbs/orapw{}".format(oracle_home, new_oracle_name))
    # Rename the control files
    os.rename("{}/dbs/{}_control1".format(oracle_home, oracle_sid), "{}/dbs/{}_control1".format(oracle_home, new_oracle_name))
    os.rename("{}/dbs/{}_control2".format(oracle_home, oracle_sid), "{}/dbs/{}_control2".format(oracle_home, new_oracle_name))
    # Change the database name in all the parameters in the init file
    status = subprocess.check_output("sed -i 's/{0}/{1}/g' {2}/dbs/init{1}.ora".format(oracle_sid, new_oracle_name, oracle_home), shell=True)
    logger.info(status.decode())
    # Switch the environment to the new database name
    os.environ["ORACLE_SID"] = new_oracle_name
    # Open the database
    logger.info(sqlplus_sysdba(oracle_home, 'create spfile from pfile;'))
    logger.info(sqlplus_sysdba(oracle_home, 'startup mount;'))
    logger.info(sqlplus_sysdba(oracle_home, 'alter database open resetlogs;'))
    return


def oracle_db_clone_cleanup(oracle_sid, oracle_home):
    os.environ["ORACLE_HOME"] = oracle_home
    os.environ["ORACLE_SID"] = oracle_sid
    logger.info(sqlplus_sysdba(oracle_home, 'shutdown abort;'))
    logger.info(sqlplus_sysdba(oracle_home, 'startup force mount exclusive restrict;'))
    logger.info(sqlplus_sysdba(oracle_home, 'drop database;'))
    delete_dbs_files(oracle_home, 'arch*')
    delete_dbs_files(oracle_home, 'c-*')
    delete_dbs_files(oracle_home, 'hc_{}.dat'.format(oracle_sid))
    delete_dbs_files(oracle_home, 'init{}.ora'.format(oracle_sid))
    delete_dbs_files(oracle_home, 'init{}.ora'.format(oracle_sid))
    delete_dbs_files(oracle_home, 'lkO{}'.format(oracle_sid))
    return


def sqlplus_sysdba(oracle_home, sql_command):
    logger.info("SQL: {}".format(sql_command))
    sql_args = [os.path.join(oracle_home, 'bin', 'sqlplus'), '-S', '/', 'as', 'sysdba']
    session = Popen(sql_args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    session.stdin.write(sql_command.encode())
    stdout, stderr = session.communicate()
    return stdout.decode()


def rman(oracle_home, rman_command):
    logger.info("RMAN: {}".format(rman_command))
    sql_args = [os.path.join(oracle_home, 'bin', 'rman'), 'target', '/']
    session = Popen(sql_args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    session.stdin.write(rman_command.encode())
    stdout, stderr = session.communicate()
    return stdout.decode()


def delete_dbs_files(oracle_home, pattern):
    file_name = glob.glob(oracle_home + '/dbs/' + pattern)
    for file_path in file_name:
        try:
            os.remove(file_path)
        except:
            logger.warning("Error while deleting file: {}".format(file_path))


def get_latest_autobackup(path):
    """
    Finds the latest control file backup recursively in a path.

   Args:
        path (str): The path in which to search for a control file.

    Returns:
        control file backup (str): The path of the latest control file backup

    """
    file_name = glob.glob(path + '/**/controlfile_c-*', recursive=True)
    if file_name:
        latest = file_name[0]
        for file_path in file_name:
            if int(file_path[-2:], base=16) > int(latest[-2:], base=16):
                latest = file_path
        return latest
    else:
        raise RubrikOracleModuleError("No control file backups were found in {}.".format(path))


class RubrikOracleScriptTimeoutError(NoTraceBackWithLineNumber):
    pass

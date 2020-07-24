#!/usr/bin/env python3
"""Module of functions for Rubrik Oracle
"""
import os
import logging
import sys
import time
import datetime
import pytz
import json
import subprocess
from subprocess import PIPE, Popen
import re
import glob
import inspect
from yaspin import yaspin
import urllib3
import rubrik_cdm

urllib3.disable_warnings()
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('rubrik_cdm').setLevel(logging.WARNING)


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


class RbsOracleCommonError(NoTraceBackWithLineNumber):
    """
    Renames object so error is named with calling script
    """
    pass


class RubrikConnection:
    """
    Creates a Rubrik connection for API commands
    """
    def __init__(self):
        self.logger = logging.getLogger(__name__ + '.RubrikConnection')
        self.logger.debug("Loading config.json files. Using credentials if present, if not using environment variables ")
        self.config = {
            'rubrik_cdm_node_ip': None,
            'rubrik_cdm_username': None,
            'rubrik_cdm_password': None,
            'rubrik_cdm_token': None
        }
        self.__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
        self.logger.debug("The config file location is {}.".format(self.__location__))
        config_file_path = os.path.join(self.__location__, 'config.json')
        if os.path.exists(config_file_path):
            with open(config_file_path) as config_file:
                self.config = json.load(config_file)
            for setting in self.config:
                if not (self.config[setting] and self.config[setting].strip()):
                    self.logger.debug("Setting {} to None".format(setting))
                    self.config[setting] = None
        self.logger.debug("Instantiating RubrikConnection using rubrik_cdm.Connect.")
        self.connection = rubrik_cdm.Connect(self.config['rubrik_cdm_node_ip'], self.config['rubrik_cdm_username'], self.config['rubrik_cdm_password'], self.config['rubrik_cdm_token'])
        self.cluster = self.connection.get('v1', '/cluster/me')
        self.name = self.cluster['name']
        self.cluster_id = self.cluster['id']
        self.timezone = self.cluster['timezone']['timezone']
        self.version = self.cluster['version']
        self.logger.info("Connected to cluster: {}, version: {}, Timezone: {}.".format(self.name, self.version, self.timezone))


class RubrikRbsOracleDatabase:
    """
    Rubrik RBS (snappable) Oracle backup object.
    """
    def __init__(self, rubrik, database_name, database_host, timeout=180):
        self.logger = logging.getLogger(__name__ + '.RubrikRbsOracleDatabase')
        self.cdm_timeout = timeout
        self.database_name = database_name
        self.database_host = database_host
        self.rubrik = rubrik
        self.oracle_id = self.get_oracle_db_id()

    def get_oracle_db_id(self):
        """
            Get the Oracle object id from the Rubrik CDM using database name and the hostname.

            This is just a wrapper on object_id function in the Rubrik CDM module.

            Args:
                self (object): Database Object
            Returns:
                oracle_db_id (str): The Rubrik database object id.
            """
        # This will use the rubrik_cdm module to get the id. There is a bug that is getting fixed so until
        # that fix is in place get the id using a basic Get.
        #     oracle_db_id = self.rubrik.connection.object_id(oracle_db_name, 'oracle_db', hostname=oracle_host_name)
        #     return oracle_db_id
        oracle_dbs = self.rubrik.connection.get("internal", "/oracle/db?name={}".format(self.database_name), timeout=self.cdm_timeout)
        # Find the oracle_db object with the correct hostName or RAC cluster name.
        # Instance names can be stored/entered with and without the domain name so
        # we will compare the hostname without the domain.
        if self.is_ip(self.database_host):
            raise RbsOracleCommonError("A hostname is required for the Oracle host, do not use an IP address.")
        oracle_id = ''
        if oracle_dbs['total'] == 0:
            raise RbsOracleCommonError(
                "The {} object '{}' was not found on the Rubrik cluster.".format(self.database_name, self.database_host))
        elif oracle_dbs['total'] > 0:
            for db in oracle_dbs['data']:
                if db['name'] == self.database_name:
                    if 'standaloneHostName' in db.keys():
                        if self.database_host == db['standaloneHostName'].split('.')[0]:
                            oracle_id = db['id']
                            break
                    elif 'racName' in db.keys():
                        if self.database_host == db['racName']:
                            oracle_id = db['id']
                            break
                        if any(instance['hostName'] == self.database_host for instance in db['instances']):
                            oracle_id = db['id']
                            break
        if oracle_id:
            self.logger.debug("Found Database id: {} for Database: {} on host or cluster {}".format(oracle_id, self.database_name, self.database_host))
            return oracle_id
        else:
            raise RbsOracleCommonError("No ID found for a database with name {} running on host {}.".format(self.database_name, self.database_host))
  
    def get_oracle_db_info(self):
        """
        Gets the information about a Rubrik Oracle database object using the Rubrik Oracle database id.

        Args:
            self (object): Database Object
        Returns:
            oracle_db_info (dict): The json returned  from the Rubrik CDM with the database information converted to a dictionary.
        """
        oracle_db_info = self.rubrik.connection.get('internal', '/oracle/db/{}'.format(self.oracle_id))
        return oracle_db_info

    def get_oracle_db_recoverable_range(self):
        """
        Gets the Rubrik Oracle database object's available recovery ranges using the Rubrik Oracle database id.

        Args:
            self (object): Database Object
        Returns:
            oracle_db_recoverable_range_info (dict): The Rubrik CDM database recovery ranges.
        """
        oracle_db_recoverable_range_info = self.rubrik.connection.get('internal', '/oracle/db/{}/recoverable_range'.format(self.oracle_id),timeout=self.cdm_timeout)
        return oracle_db_recoverable_range_info

    def get_oracle_db_snapshots(self):
        """
        Gets the Rubrik Oracle database object's available snapshots using the Rubrik Oracle database id.

        Args:
            self (object): Database Object
        Returns:
            oracle_db_recoverable_range_info (dict): The Rubrik CDM database available snapshots.
        """
        oracle_db_snapshot_info = self.rubrik.connection.get('internal', '/oracle/db/{}/snapshot'.format(self.oracle_id), timeout=self.cdm_timeout)
        return oracle_db_snapshot_info

    def oracle_db_snapshot(self, sla_id, force):
        """
        Initiates an on demand snapshot of an Oracle database. Uses the current
        SLA for that database if an sla name is not supplied.  Forces a full backup if
        force is true.

        Args:
            self (object): Database Object
            sla_id (str): The Rubrik SLA ID.
            force (bool): Force a full backup.

        Returns:
            A list of the information returned from the Rubrik CDM  from the Snapshot request.

        """
        payload = {
            "slaId": sla_id,
            "forceFullSnapshot": force
        }
        db_snapshot_info = self.rubrik.connection.post('internal', '/oracle/db/{}/snapshot'.format(self.oracle_id), payload)
        return db_snapshot_info

    def oracle_log_backup(self):
        """
        Initiates an archive log backup of an Oracle database.

        Args:
            self (object): Database Object
        Returns:
            A list of the information returned from the Rubrik CDM  from the log backup request.

        """
        oracle_log_backup_info = self.rubrik.connection.post('internal', '/oracle/db/{}/log_backup'.format(self.oracle_id), '')
        return oracle_log_backup_info

    def get_sla_id(self, sla_name):
        """
        Gets the Rubrik SLA ID for the SLA.

        Args:
            self (object): Database Object
            sla_name (str): The Rubrik SLA Domain name

        Returns:
            sla_id (str): The Rubrik SLA ID

        """
        self.logger.debug("Getting SLA information")
        sla_info = self.rubrik.connection.get('v1', '/sla_domain?name={}'.format(sla_name))
        self.logger.debug(sla_info)
        sla_id = ''
        if sla_info['total'] == 0:
            raise RbsOracleCommonError("The sla: {} was not found on this Rubrik cluster.".format(sla_name))
        elif sla_info['total'] >= 1:
            for sla in sla_info['data']:
                if sla['name'] == sla_name:
                    self.logger.debug("Matched SLA:")
                    self.logger.debug(sla)
                    sla_id = sla['id']
                    break
        if not sla_id:
            raise RbsOracleCommonError("The sla: {} was not found on this Rubrik cluster.".format(sla_name))
        return sla_id

    # New methods
    def live_mount(self, host_id, time_ms, files_only=False, mount_path=None):
        """
        Live mounts a Rubrik Database backup on the requested host or cluster.

        Args:
            self (object): Database Object
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
        live_mount_info = self.rubrik.connection.post('internal', '/oracle/db/{}/mount'.format(self.oracle_id), payload, timeout=self.cdm_timeout)
        return live_mount_info

    def get_host_id(self, primary_cluster_id, hostname):
        """
        Gets the Oracle database host using the hostname.

        Args:
            self (object): Database Object
            hostname (str): The oracle host name
            primary_cluster_id (str): The rubrik cluster id

        Returns:
            host_id (str): The host id
        """
        hostname = hostname.split('.')[0]
        host_info = self.rubrik.connection.get('internal', '/oracle/host?name={}'.format(hostname))
        host_id = ''
        if host_info['total'] > 0:
            for hosts in host_info['data']:
                if hosts['primaryClusterId'] == primary_cluster_id and hosts['status'] == 'Connected' and hosts['name'].split('.')[0] == hostname:
                    host_id = hosts['id']
                    break
        if not host_id:
            raise RbsOracleCommonError("The host: {} was not found on the Rubrik CDM.".format(hostname))
        return host_id

    def get_rac_id(self, primary_cluster_id, rac_cluster_name):
        """
        Gets the RAC Cluster ID using the cluster name.

        Args:
            self (object): Database Object
            rac_cluster_name (str): The RAC cluster name.
            primary_cluster_id (str): The rubrik cluster id

        Returns:
            rac_id (str): The RAC Cluster ID  if found otherwise will exit with error condition.
        """
        rac_info = self.rubrik.connection.get('internal', '/oracle/rac?name={}'.format(rac_cluster_name))
        rac_id = ''
        if rac_info['total'] == 0:
            raise RbsOracleCommonError(
                "The target: {} either was not found or is not a RAC cluster.".format(rac_cluster_name))
        elif rac_info['total'] > 1:
            for rac in rac_info['data']:
                if rac['primaryClusterId'] == primary_cluster_id and rac['status'] == 'Connected' and rac['name'] == rac_cluster_name:
                    rac_id = rac['id']
                    break
            # raise RubrikOracleModuleError("Multiple RAC IDs found: {} ".format(found_clusters))
        else:
            rac_id = rac_info['data'][0]['id']
        return rac_id

    def async_requests_wait(self, requests_id, timeout):
        timeout_start = time.time()
        terminal_states = ['FAILED', 'CANCELED', 'SUCCEEDED']
        while time.time() < timeout_start + (timeout * 60):
            oracle_request = self.rubrik.connection.get('internal', '/oracle/request/{}'.format(requests_id), timeout=self.cdm_timeout)
            if oracle_request['status'] in terminal_states:
                break
            with yaspin(text='Request status: {}'.format(oracle_request['status'])):
                time.sleep(10)
        if oracle_request['status'] not in terminal_states:
            raise RbsOracleCommonError(
                "\nTimeout: Async request status has been {0} for longer than the timeout period of {1} minutes. The request will remain active (current status: {0})  and the script will exit.".format(
                    oracle_request['status'], timeout))
        else:
            return oracle_request

    def oracle_db_rename(self, oracle_sid, oracle_home, new_oracle_name):
        os.environ["ORACLE_HOME"] = oracle_home
        os.environ["ORACLE_SID"] = oracle_sid
        self.logger.info(self.sqlplus_sysdba(oracle_home, 'shutdown immediate'))
        self.logger.info(self.sqlplus_sysdba(oracle_home, 'startup mount'))
        # Change the database name
        logfile = oracle_home + '/dbs/nid_' + new_oracle_name + '.log'
        self.logger.info("NID Logfile: {}".format(logfile))
        session = Popen(['nid', 'target=/', 'dbname={}'.format(new_oracle_name), 'logfile={}'.format(logfile)],
                        stdin=PIPE, stdout=PIPE, stderr=PIPE)
        stdout, stderr = session.communicate()
        self.logger.info(stdout.decode())
        # Create an init file from the spfile
        self.logger.info(self.sqlplus_sysdba(oracle_home, 'create pfile from spfile;'))
        # Rename the init file
        os.rename("{}/dbs/init{}.ora".format(oracle_home, oracle_sid),
                  "{}/dbs/init{}.ora".format(oracle_home, new_oracle_name))
        # Rename the password file if present
        if os.path.exists("{}/dbs/orapw{}".format(oracle_home, oracle_sid)):
            os.rename("{}/dbs/orapw{}".format(oracle_home, oracle_sid),
                      "{}/dbs/orapw{}".format(oracle_home, new_oracle_name))
        # Rename the control files
        os.rename("{}/dbs/{}_control1".format(oracle_home, oracle_sid),
                  "{}/dbs/{}_control1".format(oracle_home, new_oracle_name))
        os.rename("{}/dbs/{}_control2".format(oracle_home, oracle_sid),
                  "{}/dbs/{}_control2".format(oracle_home, new_oracle_name))
        # Change the database name in all the parameters in the init file
        status = subprocess.check_output(
            "sed -i 's/{0}/{1}/g' {2}/dbs/init{1}.ora".format(oracle_sid, new_oracle_name, oracle_home), shell=True)
        self.logger.info(status.decode())
        # Switch the environment to the new database name
        os.environ["ORACLE_SID"] = new_oracle_name
        # Open the database
        self.logger.info(self.sqlplus_sysdba(oracle_home, 'create spfile from pfile;'))
        self.logger.info(self.sqlplus_sysdba(oracle_home, 'startup mount;'))
        self.logger.info(self.sqlplus_sysdba(oracle_home, 'alter database open resetlogs;'))
        return

    def oracle_db_clone_cleanup(self, oracle_sid, oracle_home):
        os.environ["ORACLE_HOME"] = oracle_home
        os.environ["ORACLE_SID"] = oracle_sid
        self.logger.info(self.sqlplus_sysdba(oracle_home, 'shutdown abort;'))
        self.logger.info(self.sqlplus_sysdba(oracle_home, 'startup force mount exclusive restrict;'))
        self.logger.info(self.sqlplus_sysdba(oracle_home, 'drop database;'))
        self.delete_dbs_files(oracle_home, 'arch*')
        self.delete_dbs_files(oracle_home, 'c-*')
        self.delete_dbs_files(oracle_home, 'hc_{}.dat'.format(oracle_sid))
        self.delete_dbs_files(oracle_home, 'init{}.ora'.format(oracle_sid))
        self.delete_dbs_files(oracle_home, 'init{}.ora'.format(oracle_sid))
        self.delete_dbs_files(oracle_home, 'lkO{}'.format(oracle_sid))
        return

    def sqlplus_sysdba(self, oracle_home, sql_command):
        self.logger.info("SQL: {}".format(sql_command))
        sql_args = [os.path.join(oracle_home, 'bin', 'sqlplus'), '-S', '/', 'as', 'sysdba']
        session = Popen(sql_args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        session.stdin.write(sql_command.encode())
        stdout, stderr = session.communicate()
        return stdout.decode()

    def rman(self, oracle_home, rman_command):
        self.logger.info("RMAN: {}".format(rman_command))
        sql_args = [os.path.join(oracle_home, 'bin', 'rman'), 'target', '/']
        session = Popen(sql_args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        session.stdin.write(rman_command.encode())
        stdout, stderr = session.communicate()
        return stdout.decode()

    def delete_dbs_files(self, oracle_home, pattern):
        file_name = glob.glob(oracle_home + '/dbs/' + pattern)
        for file_path in file_name:
            try:
                os.remove(file_path)
            except:
                self.logger.warning("Error while deleting file: {}".format(file_path))

    @staticmethod
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
            raise RbsOracleCommonError("No control file backups were found in {}.".format(path))

    @staticmethod
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

    @staticmethod
    def epoch_time(iso_time_string, timezone):
        """
        Converts a time string in ISO 8601 format to epoch time using the time zone.

        Args:
            iso_time_string (str): A time string in ISO 8601 format. If the string ends with Z it is considered to be in ZULU (GMT)
            timezone (str): The timezone.
        Returns:
            epoch_time (str): the epoch time.
        """
        if iso_time_string.endswith('Z'):
            iso_time_string = iso_time_string[:-1]
            utc = pytz.utc
            datetime_object = utc.localize(datetime.datetime.fromisoformat(iso_time_string))
        else:
            cluster_timezone = pytz.timezone(timezone)
            datetime_object = cluster_timezone.localize(datetime.datetime.fromisoformat(iso_time_string))
        return int(datetime_object.timestamp()) * 1000

    @staticmethod
    def cluster_time(time_string, timezone):
        """
        Converts a time string in a timezone to a user friendly string in that time zone.

        Args:
            time_string (str): Time string.
            timezone (str): Time zone.
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


class RubrikRbsOracleMount(RubrikRbsOracleDatabase):
    """
    Rubrik RBS (snappable) Oracle backup object.
    """
    def __init__(self, rubrik, source_database_name, source_database_host,  database_mount_host):
        super().__init__(rubrik, source_database_name, source_database_host)
        self.logger = logging.getLogger(__name__ + '.RubrikRbsOracleMount')
        self.rubrik = rubrik
        self.oracle_id = self.get_oracle_db_id()
        self.database_mount_host = database_mount_host

    def get_oracle_live_mount_id(self):
        """
        This will search for and retrieve the live mount id for a live mount of the database on the host.

        Args:
            self (object): Database Object
        Returns:
            live_mount_id (str): The id of the requested live mount.
        """
        oracle_live_mounts = self.rubrik.connection.get('internal', '/oracle/db/mount?source_database_id={}'.format(self.oracle_id))
        live_mount_id = []
        # On CDM 5.1.1+ the targetHostID id is the host or cluster name first.
        for mount in oracle_live_mounts['data']:
            if self.database_mount_host in mount['targetHostId']:
                live_mount_id.append(mount['id'])
        if live_mount_id:
            return live_mount_id
        # If no match the CDM release is pre 5.1.1 and we much find the id for the target host
        # Check if host_cluster is a RAC Cluster or a node in a RAC cluster so we can use the RAC cluster id
        rac_id = self.rubrik.connection.get('internal', '/oracle/rac?name={}'.format(self.database_mount_host))
        mount_host_id = ''
        if rac_id['total'] == 0:
            rac_info = self.rubrik.connection.get('internal', '/oracle/rac')
            for rac in rac_info['data']:
                for nodes in rac['nodes']:
                    if nodes['nodeName'] == self.database_mount_host:
                        mount_host_id = rac['id']
                if mount_host_id:
                    break
        else:
            for rac in rac_id['data']:
                if rac['primaryClusterId'] == self.rubrik.cluster_id and rac['status'] == 'Connected' and rac['name'] == self.database_mount_host:
                    mount_host_id = rac['id']
        if not mount_host_id:
            mount_host_id = self.get_host_id(self.rubrik.cluster_id, self.database_mount_host)
        host_id = mount_host_id.split(':::')[1]
        for mount in oracle_live_mounts['data']:
            if host_id == mount['targetHostId']:
                live_mount_id.append(mount['id'])
                self.logger.info("Live mount id: {}".format(live_mount_id))
        return live_mount_id

    def get_live_mount_info(self, live_mount_id):
        """
        Gets all the information about the live mount using the live mount id.
        """
        live_mount_info = self.rubrik.connection.get('internal', '/oracle/db/mount/{}'.format(live_mount_id))
        return live_mount_info

    def live_mount_delete(self, live_mount_id, force):
        """
        This will unmount a live mounted database or backup set.

        Args:
            self (object): Database Object
            live_mount_id (str): The id of the mount to remove,
            force (bool): Set to true to force the unmount.

        Returns:
            live_mount_delete_info (dict): The information returned from the Rubrik CDM about the requested unmount.
        """
        live_mount_delete_info = self.rubrik.connection.delete('internal', '/oracle/db/mount/{}?force={}'.format(live_mount_id, force))
        return live_mount_delete_info

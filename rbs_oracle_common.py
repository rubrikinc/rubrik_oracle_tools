#!/usr/bin/env python3
"""Module of functions for Rubrik Oracle
"""
import os
import logging
import sys
import datetime
import pytz
import json
import subprocess
from subprocess import PIPE, Popen
import re
import inspect
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


class RubrikOracleModuleError(NoTraceBackWithLineNumber):
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
        self.__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
        self.logger.debug("Loading config.json files. Using credentials if present, if not using environment variables ")
        with open(os.path.join(self.__location__, 'config.json')) as config_file:
            self.config = json.load(config_file)
        if not self.config['rubrik_cdm_node_ip']:
            self.config['rubrik_cdm_node_ip'] = None
        self.logger.debug("Instantiating RubrikConnection using rubrik_cdm.Connect.")
        self.connection = rubrik_cdm.Connect(self.config['rubrik_cdm_node_ip'], self.config['rubrik_cdm_username'], self.config['rubrik_cdm_password'], self.config['rubrik_cdm_token'])
        self.cluster = self.connection.get('v1', '/cluster/me')
        self.timezone = self.cluster['timezone']['timezone']
        self.logger.info("Connected to cluster: {}, version: {}, Timezone: {}.".format(self.cluster['name'], self.cluster['version'], self.timezone))


class RubrikRbsOracleDatabase:
    """
    Rubrik RBS (snappable) Oracle backup object.
    """
    def __init__(self, rubrik, database_name, database_host):
        self.logger = logging.getLogger(__name__ + '.RubrikRbsOracleDatabase')
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
        oracle_dbs = self.rubrik.connection.get("internal", "/oracle/db?name={}".format(self.database_name), timeout=60)
        # Find the oracle_db object with the correct hostName or RAC cluster name.
        # Instance names can be stored/entered with and without the domain name so
        # we will compare the hostname without the domain.
        if self.is_ip(self.database_host):
            raise RubrikOracleModuleError("A hostname is required for the Oracle host, do not use an IP address.")
        if oracle_dbs['total'] == 0:
            raise RubrikOracleModuleError(
                "The {} object '{}' was not found on the Rubrik cluster.".format(self.database_name, self.database_host))
        elif oracle_dbs['total'] > 0:
            for db in oracle_dbs['data']:
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
        self.logger.debug("Found Database id: {} for Database: {} on host or cluster {}".format(oracle_id, self.database_name, self.database_host))
        return oracle_id

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
        oracle_db_recoverable_range_info = self.rubrik.connection.get('internal', '/oracle/db/{}/recoverable_range'.format(self.oracle_id),timeout=60)
        return oracle_db_recoverable_range_info

    def get_oracle_db_snapshots(self):
        """
        Gets the Rubrik Oracle database object's available snapshots using the Rubrik Oracle database id.

        Args:
            self (object): Database Object
        Returns:
            oracle_db_recoverable_range_info (dict): The Rubrik CDM database available snapshots.
        """
        oracle_db_snapshot_info = self.rubrik.connection.get('internal', '/oracle/db/{}/snapshot'.format(self.oracle_id), timeout=60)
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
        sla_info = self.rubrik.connection.get('v1', '/sla_domain?name={}'.format(sla_name))
        if sla_info['total'] == 0:
            raise RubrikOracleModuleError("The sla: {} was not found on this Rubrik cluster.".format(sla_name))
        elif sla_info['total'] > 1:
            raise RubrikOracleModuleError("Multiple SLAs with the name {} were found on this cluster.".format(sla_name))
        else:
            sla_id = sla_info['data'][0]['id']
        return sla_id

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

    # @staticmethod
    # def cluster_time(iso_time_string, timezone):
    #     """
    #     Converts a time string in a timezone to a user friendly string in that time zone.
    #
    #     Args:
    #         iso_time_string (str): Time string.
    #         timezone (str): Time zone.
    #     Returns:
    #         time_string (str): Time string converted to the supplied time zone.
    #     """
    #     cluster_timezone = pytz.timezone(timezone)
    #     utc = pytz.utc
    #     if iso_time_string.endswith('Z'):
    #         time_string = iso_time_string[:-1]
    #         datetime_object = utc.localize(datetime.datetime.fromisoformat(iso_time_string))
    #     else:
    #         datetime_object = cluster_timezone.localize(datetime.datetime.fromisoformat(iso_time_string))
    #     cluster_time_object = cluster_timezone.normalize(datetime_object.astimezone(cluster_timezone))
    #     return cluster_time_object.isoformat()

    @staticmethod
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


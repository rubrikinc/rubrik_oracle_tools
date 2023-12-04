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
import base64
import subprocess
from subprocess import PIPE, Popen
import re
import glob
import inspect
from yaspin import yaspin
from yaspin.spinners import Spinners
import urllib3
import rubrik_cdm
from dataclasses import dataclass, field
from typing import Callable, ClassVar, Dict, Optional
import requests

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
    def __init__(self, keyfile=None, insecure=False):
        self.logger = logging.getLogger(__name__ + '.RubrikConnection')
        if keyfile:
            self.logger.debug(
                "Using keyfile {} for auth.".format(keyfile))
            if os.path.exists(keyfile):
                with open(keyfile) as config_file:
                    self.config = json.load(config_file)
                for setting in self.config:
                    if not (self.config[setting] and self.config[setting].strip()):
                        self.config[setting] = None
                if self.config['vault_uri']:
                    self.config['rubrik_cdm_node_ip'] = self.config['vault_uri']
                else:
                    self.config['rubrik_cdm_node_ip'] = os.environ.get('rubrik_cdm_node_ip')
                if not self.config['rubrik_cdm_node_ip']:
                    raise RbsOracleCommonError("A Rubrik Vault URI is required for this connection.")
                self.config['rubrik_cdm_node_ip'] = self.config['rubrik_cdm_node_ip'].strip('https://')
                self.logger.warning("Using service account...")
                self.service_account = True
                self.get_sa_token()
            else:
                raise RbsOracleCommonError("No keyfile found at {}".format(keyfile))
        else:
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
                        self.config[setting] = None
            self.service_account = False
            if not self.config['rubrik_cdm_node_ip']:
                self.config['rubrik_cdm_node_ip'] = os.environ.get('rubrik_cdm_node_ip')
            if not self.config['rubrik_cdm_token']:
                self.config['rubrik_cdm_token'] = os.environ.get('rubrik_cdm_token')
            if not self.config['rubrik_cdm_username']:
                self.config['rubrik_cdm_username'] = os.environ.get('rubrik_cdm_username')
            if not self.config['rubrik_cdm_password']:
                self.config['rubrik_cdm_password'] = os.environ.get('rubrik_cdm_password')
            if not self.config['rubrik_cdm_node_ip']:
                raise RbsOracleCommonError("No Rubrik CDM IP or URL supplied in either the config file or environmental variables.")

            if not self.config['rubrik_cdm_token'] and not (self.config['rubrik_cdm_username'] and self.config['rubrik_cdm_password']):
                raise RbsOracleCommonError("No Rubrik user/password(Service Account) or token supplied in either the config file or environmental variables.")
            # Check if user is a service account
            if self.config['rubrik_cdm_username']:
                if "User:::" in self.config['rubrik_cdm_username']:
                    self.logger.warning("Using service account...")
                    self.service_account = True
                    self.logger.debug("client_id: {}, client_secret: {}".format(self.config['rubrik_cdm_username'], self.config['rubrik_cdm_password']))
                    self.config['client_id'] = self.config['rubrik_cdm_username']
                    self.config['client_secret'] = self.config['rubrik_cdm_password']
                    self.get_sa_token()
                    self.config['rubrik_cdm_username'] = None
                    self.config['rubrik_cdm_password'] = None

        self.logger.debug("Instantiating RubrikConnection using rubrik_cdm.Connect.")
        if self.service_account:
            self.connection = rubrik_cdm.Connect(self.config['rubrik_cdm_node_ip'], None, None, self.config['rubrik_cdm_token'])
        else:
            self.connection = rubrik_cdm.Connect(self.config['rubrik_cdm_node_ip'], self.config['rubrik_cdm_username'], self.config['rubrik_cdm_password'], self.config['rubrik_cdm_token'])

        self.cluster = self.connection.get('v1', '/cluster/me')
        self.name = self.cluster['name']
        self.cluster_id = self.cluster['id']
        self.timezone = self.cluster['timezone']['timezone']
        self.version = self.cluster['version']
        self.logger.info("Connected to cluster: {}, version: {}, Timezone: {}.".format(self.name, self.version, self.timezone))

    def get_sa_token(self):
        payload = {
            "serviceAccountId": self.config['client_id'],
            "secret": self.config['client_secret']
        }
        payload = json.dumps(payload)
        _headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        token_uri = "https://" + self.config['rubrik_cdm_node_ip'] + "/api/v1/service_account/session"
        self.logger.debug("Token URI: {}".format(token_uri))
        response = requests.post(
            token_uri,
            verify=False,
            data=payload,
            headers=_headers
        )

        self.logger.debug("Requests response: {}".format(response))
        response_json = response.json()
        self.logger.debug("Requests response json: {}".format(response_json))
        if 'token' not in response_json:
            raise RbsOracleCommonError("Unable to create session and retrieve token. Error: {}".format(response_json['message']))
        else:
            self.logger.debug("Access Token returned.")

        self.config['rubrik_cdm_token'] = response_json['token']

    def delete_session(self):
        if self.service_account:
            self.logger.debug("Deleting Session")
            response = None
            try:
                response = self.connection.delete('v1', '/session/me')
            except:
                self.logger.warning("Unable to delete session: Token was not released...")
                return
            self.logger.debug("Session delete response: {}".format(response))
            self.logger.warning("Service account session deleted.")



class RubrikRbsOracleDatabase:
    """
    Rubrik RBS (snappable) Oracle backup object.
    """
    def __init__(self, rubrik, database_name, database_host, timeout=180, id=None):
        self.logger = logging.getLogger(__name__ + '.RubrikRbsOracleDatabase')
        self.cdm_timeout = timeout
        self.database_name = database_name
        self.database_host = database_host
        self.rubrik = rubrik
        if int(self.rubrik.version.split("-")[0].split(".")[0]) >= 6:
            self.v6 = True
            self.v6_deprecated = 'v1'
        else:
            self.v6 = False
            self.v6_deprecated = 'internal'
        if id:
            self.oracle_id = id
        else:
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
        if self.is_ip(self.database_host):
            self.rubrik.delete_session()
            raise RbsOracleCommonError("A hostname is required for the Oracle host, do not use an IP address.")
        if int(self.rubrik.version.split("-")[0].split(".")[0]) >= 8:
            self.logger.debug("Using graphql queries with CDM version: {}".format(self.rubrik.version))
            query = """query OracleDatabase($name: String, $effectiveSlaDomainId: String, $slaAssignment: String, $primaryClusterId: String, $isRelic: Boolean, $shouldIncludeDataGuardGroups: Boolean, $first: Int, $after: String, $sortBy: String, $sortOrder: String) {
                          oracleDatabaseConnection(name: $name, effectiveSlaDomainId: $effectiveSlaDomainId, slaAssignment: $slaAssignment, primaryClusterId: $primaryClusterId, isRelic: $isRelic, shouldIncludeDataGuardGroups: $shouldIncludeDataGuardGroups, first: $first, after: $after, sortBy: $sortBy, sortOrder: $sortOrder) {
                            nodes {
                              id
                              name
                              sid
                              racId
                              databaseRole
                              dbUniqueName
                              dataGuardGroupId
                              dataGuardGroupName
                              standaloneHostId
                              primaryClusterId
                              slaAssignment
                              configuredSlaDomainId
                              configuredSlaDomainName
                              effectiveSlaDomain {
                                id
                                name
                                sourceId
                                sourceName
                                polarisManagedId
                                isRetentionLocked
                              }
                              infraPath {
                                id
                                name
                              }
                              isRelic
                              numInstances
                              instances {
                                hostName
                                instanceSid
                              }
                              isArchiveLogModeEnabled
                              standaloneHostName
                              racName
                              numTablespaces
                              logBackupFrequencyInMinutes
                            }
                          }
                        }"""
            variables = {
                "name": self.database_name,
                "sortOrder": "asc",
                "shouldIncludeDataGuardGroups": True
            }
            payload = {"query": query, "variables": variables}
            databases = self.rubrik.connection.post('internal', '/graphql', payload)['data']['oracleDatabaseConnection']['nodes']
            self.logger.debug("Returned databases: {}".format(databases))
            id = None
            if len(databases) == 0:
                self.logger.debug("No database found for database name {}, checking for database unique name...".format(
                    self.database_name))
                variables = {
                    "shouldIncludeDataGuardGroups": True
                }
                payload = {"query": query, "variables": variables}
                all_dbs = self.rubrik.connection.post('internal', '/graphql', payload)['data']['oracleDatabaseConnection']['nodes']
                self.logger.debug("all_dbs: {}".format(all_dbs))
                for db in all_dbs:
                    if db['dbUniqueName'].lower() == self.database_name.lower():
                        self.logger.debug("Found object with dbUniqueName: {}".format(db))
                        databases.append(db)
                self.logger.debug("Databases found for database unique name {}: {}".format(self.database_name, databases))
            if len(databases) == 0:
                self.rubrik.delete_session()
                raise RbsOracleCommonError("No snapshots found for database: {}".format(self.database_name))
            elif len(databases) == 1:
                if databases[0]['dataGuardGroupId']:
                    id = databases[0]['dataGuardGroupId']
                else:
                    id = databases[0]['id']
            else:
                for db in databases:
                    if db['standaloneHostName']:
                        self.logger.debug("Database hosts to match: {}, {}".format(self.database_host, db['standaloneHostName']))
                        if self.match_hostname(self.database_host,
                                                                                    db['standaloneHostName']):
                            if db['dataGuardGroupId']:
                                id = db['dataGuardGroupId']
                            else:
                                id = db['id']
                    elif db['racName']:
                        self.logger.debug("Database RAC names to match: {}, {}".format(self.database_host, db['racName']))
                        if self.match_hostname(self.database_host, db['racName']):
                            if db['dataGuardGroupId']:
                                id = db['dataGuardGroupId']
                            else:
                                id = db['id']
                if not id:
                    id_list = []
                    for db in databases:
                        if db['dataGuardGroupId']:
                            id_list.append(db['dataGuardGroupId'])
                        else:
                            id_list.append(db['id'])
                    if not id_list:
                        self.rubrik.delete_session()
                        raise RbsOracleCommonError("Unable to extract id from Multiple databases returned for database name: {} with no hostname/rac match".format(self.database_name))
                    if id_list.count(id_list[0]) == len(id_list):
                        id = id_list[0]
                    else:
                        self.rubrik.delete_session()
                        raise RbsOracleCommonError("Unable to extract id from Multiple databases returned for database name: {} with no hostname/rac match".format(self.database_name))
                if not id:
                    self.rubrik.delete_session()
                    raise RbsOracleCommonError("Multiple database's snapshots found for database name: {}".format(self.database_name))
            if not id:
                self.rubrik.delete_session()
                raise RbsOracleCommonError("No ID found for a database with name {} running on host {}.".format(self.database_name,self.database_host))
            return id
        else:
            self.logger.debug("Using REST API GET with CDM version: {}".format(self.rubrik.version))
            oracle_dbs = self.rubrik.connection.get(self.v6_deprecated, "/oracle/db?name={}".format(self.database_name),
                                                    timeout=self.cdm_timeout)
            self.logger.debug("Oracle DBs with name: {} returned: {}".format(self.database_name, oracle_dbs))
            # Find the oracle_db object with the correct hostName or RAC cluster name.
            # Instance names can be stored/entered with and without the domain name so
            # we will compare the hostname without the domain.
            if self.is_ip(self.database_host):
                raise RbsOracleCommonError("A hostname is required for the Oracle host, do not use an IP address.")
            oracle_id = None
            if oracle_dbs['total'] == 0 and self.v6:
                self.logger.debug("No database found for database name {}, checking for database unique name...".format(
                    self.database_name))
                all_dbs = self.rubrik.connection.get(self.v6_deprecated, "/oracle/db".format(self.database_name),
                                                     timeout=self.cdm_timeout)
                for db in all_dbs['data']:
                    if db['dbUniqueName'].lower() == self.database_name.lower():
                        self.logger.debug("Found object with dbUniqueName: {}".format(db))
                        oracle_dbs['data'].append(db)
                        oracle_dbs['total'] += 1
                        self.db_unique_name = True
                self.logger.debug(
                    "Databases found for database unique name {}: {}".format(self.database_name, oracle_dbs))
            if oracle_dbs['total'] == 0:
                raise RbsOracleCommonError(
                    "The {} object '{}' was not found on the Rubrik cluster.".format(self.database_name,
                                                                                     self.database_host))
            elif oracle_dbs['total'] > 0:
                for db in oracle_dbs['data']:
                    if (db['name'].lower() == self.database_name.lower() or db[
                        'dbUniqueName'].lower() == self.database_name.lower()) and db['isRelic'] == False:
                        if 'standaloneHostName' in db.keys():
                            if self.match_hostname(self.database_host, db['standaloneHostName']):
                                oracle_id = db['id']
                                if self.v6:
                                    if db['dataGuardType'] == 'DataGuardMember':
                                        oracle_id = db['dataGuardGroupId']
                                break
                        elif 'racName' in db.keys():
                            if self.database_host == db['racName']:
                                oracle_id = db['id']
                                if self.v6:
                                    if db['dataGuardType'] == 'DataGuardMember':
                                        oracle_id = db['dataGuardGroupId']
                                break
                            for instance in db['instances']:
                                if self.match_hostname(self.database_host, instance['hostName']):
                                    oracle_id = db['id']
                                    if self.v6:
                                        if db['dataGuardType'] == 'DataGuardMember':
                                            oracle_id = db['dataGuardGroupId']
                                    break
                            if oracle_id:
                                break
            if oracle_id:
                self.logger.debug(
                    "Found Database id: {} for Database: {} on host or cluster {}".format(oracle_id, self.database_name,
                                                                                          self.database_host))
                return oracle_id
            else:
                if self.db_unique_name:
                    raise RbsOracleCommonError(
                        "No ID found for a database with DB Unique Name {} running on host {}.".format(
                            self.database_name, self.database_host))
                else:
                    raise RbsOracleCommonError(
                        "No ID found for a database with name {} running on host {}.".format(self.database_name,
                                                                                             self.database_host))


    def get_oracle_db_info(self):
        """
        Gets the information about a Rubrik Oracle database object using the Rubrik Oracle database id.

        Args:
            self (object): Database Object
        Returns:
            oracle_db_info (dict): The json returned  from the Rubrik CDM with the database information converted to a dictionary.
        """
        oracle_db_info = self.rubrik.connection.get(self.v6_deprecated, '/oracle/db/{}'.format(self.oracle_id), timeout=self.cdm_timeout)
        return oracle_db_info

    def get_oracle_db_recoverable_range(self):
        """
        Gets the Rubrik Oracle database object's available recovery ranges using the Rubrik Oracle database id.

        Args:
            self (object): Database Object
        Returns:
            oracle_db_recoverable_range_info (dict): The Rubrik CDM database recovery ranges.
        """
        oracle_db_recoverable_range_info = self.rubrik.connection.get('internal', '/oracle/db/{}/recoverable_range'.format(self.oracle_id), timeout=self.cdm_timeout)
        return oracle_db_recoverable_range_info

    def get_oracle_db_snapshots(self):
        """
        Gets the Rubrik Oracle database object's available snapshots using the Rubrik Oracle database id.

        Args:
            self (object): Database Object
        Returns:
            oracle_db_recoverable_range_info (dict): The Rubrik CDM database available snapshots.
        """
        self.logger.debug("API call: internal/oracle/db/{}/snapshot".format(self.oracle_id))
        try:
            oracle_db_snapshot_info = self.rubrik.connection.get('internal', '/oracle/db/{}/snapshot'.format(self.oracle_id), timeout=self.cdm_timeout)
        except Exception as err:
            self.rubrik.delete_session()
            raise RbsOracleCommonError(f"Method get_oracle_db_snapshots failed for id: {self.oracle_id} with Unexpected {err=}, {type(err)=}")
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
        db_snapshot_info = self.rubrik.connection.post('internal', '/oracle/db/{}/snapshot'.format(self.oracle_id), payload, timeout=self.cdm_timeout)
        return db_snapshot_info

    def oracle_log_backup(self):
        """
        Initiates an archive log backup of an Oracle database.

        Args:
            self (object): Database Object
        Returns:
            A list of the information returned from the Rubrik CDM  from the log backup request.

        """
        oracle_log_backup_info = self.rubrik.connection.post('internal', '/oracle/db/{}/log_backup'.format(self.oracle_id), '', timeout=self.cdm_timeout)
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

    def live_mount(self, host_id, time_ms, files_only=False, mount_path=None, pfile=None, aco_config_map=None, oracle_home=None):
        """
        Live mounts a Rubrik Database backup on the requested host or cluster.

        Args:
            self (object): Database Object
            host_id (str):  The Rubrik host or cluster for the mount.
            time_ms  (str):  The point in time of the backup to mount.
            files_only (bool):  Mount the backup pieces only.
            mount_path (str):  The path to mount the files only restore. (Required if files_only is True).
            pfile (str): The path to the custom pfile to use on the live mount host (mutually exclusive with ACO file).
            aco_file (str): The base64 encoded ACO file read into the variable.
            aco_parameters (dict): The ACO parameters as read from the ACO file.
            oracle_home: ACO parameter for $ORACLE_HOME. Required (if not in aco_file) for DG Groups

        Returns:
            live_mount_info (dict): The information about the requested live mount returned from the Rubrik CDM.
        """
        payload = {
                "recoveryPoint": {"timestampMs": time_ms},
                "targetOracleHostOrRacId": host_id,
                "targetMountPath": mount_path,
                "shouldMountFilesOnly": files_only
            }
        if pfile:
            payload["customPfilePath"] = pfile.replace("'", "")
        if oracle_home:
            self.logger.debug("Oracle Home provided: {0}".format(oracle_home))
            aco_config_map = {}
            aco_config_map['ORACLE_HOME'] = oracle_home
        if aco_config_map:
            payload["advancedRecoveryConfigMap"] = aco_config_map
        self.logger.debug("RBS oracle common payload: {}".format(payload))
        try:
            live_mount_info = self.rubrik.connection.post('internal', '/oracle/db/{}/mount'.format(self.oracle_id), payload, timeout=self.cdm_timeout)
        except Exception as err:
            self.rubrik.delete_session()
            raise RbsOracleCommonError(f"Method live_mount_info failed for id: {self.oracle_id} with Unexpected {err=}, {type(err)=}")
        return live_mount_info

    def db_clone(self, host_id, time_ms, files_only=False, mount_path=None, new_name=None, pfile=None, aco_parameters=None, oracle_home=None):
        """
        Clones an Oracle database using RBS automation.

        Args:
            self (object): Database Object
            host_id (str):  The Rubrik host or cluster for the mount.
            time_ms  (str):  The point in time of the backup to mount.
            files_only (bool):  Mount the backup pieces only.
            mount_path (str):  The path to mount the files only restore. (Required if files_only is True).
            new_name (str): New name for clone db (optional). Requires either an ACO file or a custom pfile.
            pfile (str): The path to the custom pfile to use on the live mount host (mutually exclusive with ACO file).
            aco_file (str): The base64 encoded ACO file read into the variable.
            aco_parameters (list): The ACO parameters as read from the ACO file.
            oracle_home: ACO parameter for $ORACLE_HOME. Required (if not in aco_file) for DG Groups

        Returns:
            db_clone_info (dict): The information about the requested clone returned from the Rubrik CDM.
        """
        payload = {
                "recoveryPoint": {"timestampMs": time_ms},
                "targetOracleHostOrRacId": host_id,
                "targetMountPath": mount_path,
                "shouldMountFilesOnly": files_only
            }
        if new_name:
            payload["cloneDbName"] = new_name.replace("'", "")
        if pfile:
            payload["customPfilePath"] = pfile.replace("'", "")
        if aco_parameters:
            payload["advancedRecoveryConfigMap"] = {}
            for parameter in aco_parameters:
                if "_CONVERT" not in parameter[0].upper():
                    stripped_parameter = parameter[1].replace("'", "")
                    stripped_parameter = stripped_parameter.replace('"', '')
                else:
                    stripped_parameter = parameter[1]
                payload["advancedRecoveryConfigMap"][parameter[0]] = stripped_parameter
        if oracle_home:
            if not aco_parameters:
                payload["advancedRecoveryConfigMap"] = {"ORACLE_HOME": oracle_home.replace("'", "")}
            else:
                payload["advancedRecoveryConfigMap"]["ORACLE_HOME"] = oracle_home.replace("'", "")
        self.logger.debug("RBS oracle common payload: {}".format(payload))
        try:
            db_clone_info = self.rubrik.connection.post('internal', '/oracle/db/{}/export'.format(self.oracle_id), payload, timeout=self.cdm_timeout)
        except Exception as err:
            self.rubrik.delete_session()
            raise RbsOracleCommonError(f"Method db_clone_info failed for id: {self.oracle_id} with Unexpected {err=}, {type(err)=}")
        return db_clone_info

    def oracle_validate(self, host_id, time_ms):
        """
        Validates  Rubrik Database backup on the requested host or cluster or source host.

        Args:
            self (object): Database Object
            host_id (str):  The Rubrik host or cluster for the mount.
            time_ms  (str):  The point in time of the backup to mount.

        Returns:
            oracle_validate_info (dict): The information about the requested database validate returned from the Rubrik CDM.
        """
        payload = {
            "recoveryPoint": {"timestampMs": time_ms},
            "targetOracleHostOrRacId": host_id
        }
        oracle_validate_info = self.rubrik.connection.post('v1', '/oracle/db/{}/validate'.format(self.oracle_id), payload, timeout=self.cdm_timeout)
        return oracle_validate_info

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
        host_info = self.rubrik.connection.get('internal', '/oracle/host?name={}'.format(hostname), timeout=self.cdm_timeout)
        self.logger.debug("host_info returned for hostname {}: {}".format(hostname,host_info))
        host_id = ''
        matched_hosts = []
        if host_info['total'] > 0:
            for host in host_info['data']:
                if host['primaryClusterId'] == primary_cluster_id and host['status'] == 'Connected' and hostname in host['name'] :
                    matched_hosts.append(host)
        if len(matched_hosts) == 0:
            self.rubrik.delete_session()
            raise RbsOracleCommonError("The host: {} was not found on the Rubrik CDM.".format(hostname))
        elif len(matched_hosts) == 1:
            host_id = matched_hosts[0]['id']
        else:
            self.rubrik.delete_session()
            raise RbsOracleCommonError("Multiple hosts with name: {} was found on the Rubrik CDM. Try using full FQDN.".format(hostname))
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
        rac_info = self.rubrik.connection.get('internal', '/oracle/rac?name={}'.format(rac_cluster_name), timeout=self.cdm_timeout)
        rac_id = ''
        if rac_info['total'] == 0:
            self.rubrik.delete_session()
            raise RbsOracleCommonError(
                "The target: {} either was not found or is not a RAC cluster.".format(rac_cluster_name))
        elif rac_info['total'] > 1:
            for rac in rac_info['data']:
                if rac['primaryClusterId'] == primary_cluster_id and rac['status'] == 'Connected' and rac['name'] == rac_cluster_name:
                    rac_id = rac['id']
                    break
        else:
            rac_id = rac_info['data'][0]['id']
        return rac_id

    def get_target_id(self, primary_cluster_id, target_name):
        """
        Gets the RAC Cluster ID or the Host ID using the target name.

        Args:
            self (object): Database Object
            target_name (str): The target name.
            primary_cluster_id (str): The rubrik cluster id

        Returns:
            target_id (str): The RAC or Host ID  if found otherwise will exit with error condition.
        """
        rac_info = self.rubrik.connection.get('internal', '/oracle/rac?name={}'.format(target_name), timeout=self.cdm_timeout)
        target_id = ''
        if rac_info['total'] == 1 and rac_info['data'][0]['name'] == target_name:
            target_id = rac_info['data'][0]['id']
        elif rac_info['total'] > 1:
            for rac in rac_info['data']:
                if rac['primaryClusterId'] == primary_cluster_id and rac['status'] == 'Connected' and rac['name'] == target_name:
                    target_id = rac['id']
                    break
        else:
            target_name = target_name.split('.')[0]
            host_info = self.rubrik.connection.get('internal', '/oracle/host?name={}'.format(target_name), timeout=self.cdm_timeout)
            if host_info['total'] > 0:
                for hosts in host_info['data']:
                    if hosts['primaryClusterId'] == primary_cluster_id and hosts['status'] == 'Connected' and hosts['name'].split('.')[0] == target_name:
                        target_id = hosts['id']
                        break
        if not target_id:
            self.rubrik.delete_session()
            raise RbsOracleCommonError("The host or RAC cluster: {} was not found on the Rubrik CDM.".format(target_name))
        return target_id

    def get_any_rac_target_id(self, primary_cluster_id, target_name):
        """
        Gets the RAC Cluster ID or the Host ID using the target name.

        Args:
            self (object): Database Object
            target_name (str): The target name.
            primary_cluster_id (str): The rubrik cluster id

        Returns:
            target_id (str): The RAC or Host ID  if found otherwise will exit with error condition.
        """
        target_name = target_name.split('.')[0]
        host_info = self.rubrik.connection.get('internal', '/oracle/host?name={}'.format(target_name),
                                               timeout=self.cdm_timeout)
        target_id = ''
        if host_info['total'] > 0:
            self.logger.debug("Hostnames were matched.")
            for hosts in host_info['data']:
                if hosts['primaryClusterId'] == primary_cluster_id and hosts['status'] == 'Connected' and \
                        hosts['name'].split('.')[0] == target_name:
                    target_id = hosts['id']
                    self.logger.debug(f"Target id: {target_id} found from hostname match.")
                    break
        else:
            self.logger.debug("Checking for RAC name.")
            rac_info = self.rubrik.connection.get('internal', '/oracle/rac?name={}'.format(target_name),
                                                  timeout=self.cdm_timeout)

            if rac_info['total'] == 1 and rac_info['data'][0]['name'] == target_name:
                target_id = rac_info['data'][0]['id']
                self.logger.debug(f"Target id: {target_id} found from rac name match.")
            elif rac_info['total'] > 1:
                for rac in rac_info['data']:
                    if rac['primaryClusterId'] == primary_cluster_id and rac['status'] == 'Connected' and rac[
                        'name'] == target_name:
                        target_id = rac['id']
                        self.logger.debug(f"Target id: {target_id} found from rac name match.")
                        break
            else:
                self.logger.debug("Checking for RAC name using the target hostname.")
                rac_info = self.rubrik.connection.get('internal', '/oracle/rac', timeout=self.cdm_timeout)
                self.logger.debug(f"All RACs returned: {rac_info}")
                if rac_info['total'] > 0:
                    for rac in rac_info['data']:
                        if rac['primaryClusterId'] == primary_cluster_id and rac['status'] == 'Connected' and \
                                rac['name'].split('.')[0] == target_name:
                            target_id = rac['id']
                            break
                        elif rac['primaryClusterId'] == primary_cluster_id and rac['status'] == 'Connected':
                            for node in rac['nodes']:
                                if node['nodeName'].split('.')[0] == target_name:
                                    target_id = rac['id']
                                    break

        if not target_id:
            self.rubrik.delete_session()
            raise RbsOracleCommonError("The host: {} was not found on the Rubrik CDM.".format(target_name))
        return target_id

    def async_requests_wait(self, requests_id, timeout):
        timeout_start = time.time()
        terminal_states = ['FAILED', 'CANCELED', 'SUCCEEDED']
        oracle_request = None
        while time.time() < timeout_start + (timeout * 60):
            oracle_request = self.rubrik.connection.get('internal', '/oracle/request/{}'.format(requests_id), timeout=self.cdm_timeout)
            if oracle_request['status'] in terminal_states:
                break
            with yaspin(Spinners.line, text='Request status: {}'.format(oracle_request['status'])):
                time.sleep(10)
        if oracle_request['status'] not in terminal_states:
            self.rubrik.delete_session()
            raise RbsOracleCommonError(
                "\nTimeout: Async request status has been {0} for longer than the timeout period of {1} minutes. The request will remain active (current status: {0})  and the script will exit.".format(
                    oracle_request['status'], timeout))
        else:
            return oracle_request

    def async_sla_change_wait(self, pending_sla, timeout):
        timeout_start = time.time()
        oracle_request = None
        db_info = self.get_oracle_db_info()
        if pending_sla == 'inherit':
            while time.time() < timeout_start + (timeout * 60):
                db_info = self.get_oracle_db_info()
                if db_info['slaAssignment'] == 'Derived':
                    break
                with yaspin(Spinners.line, text='Effective SLA: {}'.format(db_info['effectiveSlaDomainName'])):
                    time.sleep(10)
            if db_info['effectiveSlaDomainName'] == 'Unprotected':
                self.rubrik.delete_session()
                raise RbsOracleCommonError(
                    "\nTimeout: Async request status has been {0} for longer than the timeout period of {1} minutes. The request will remain active (current effective SLA: {0})  and the script will exit.".format(
                        db_info['effectiveSlaDomainName'], timeout))
            else:
                return db_info
        else:
            while time.time() < timeout_start + (timeout * 60):
                db_info = self.get_oracle_db_info()
                if db_info['effectiveSlaDomainName'] == pending_sla:
                    break
                with yaspin(Spinners.line, text='Effective SLA: {}'.format(db_info['effectiveSlaDomainName'])):
                    time.sleep(10)
            if db_info['effectiveSlaDomainName'] != pending_sla:
                self.rubrik.delete_session()
                raise RbsOracleCommonError(
                    "\nTimeout: Async request status has been {0} for longer than the timeout period of {1} minutes. The request will remain active (current effective SLA: {0})  and the script will exit.".format(
                        db_info['effectiveSlaDomainName'], timeout))
            else:
                return db_info

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

    def rman(self, oracle_home, rman_command, target="target"):
        self.logger.info("RMAN: {}".format(rman_command))
        rman_args = [os.path.join(oracle_home, 'bin', 'rman'), target, '/']
        session = Popen(rman_args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
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

    def refresh(self):
        """
                Refreshes Oracle database in CDM.

                Args:
                    self (object): Database Object
                Returns:
                    response (str): The response json from the refresh
                """
        oracle_database_refresh_info = self.rubrik.connection.post('v1', '/oracle/db/{0}/refresh'.format(self.oracle_id), '', timeout=self.cdm_timeout)
        self.logger.debug("Refresh function response: {0}".format(oracle_database_refresh_info))
        return oracle_database_refresh_info

    def oracle_db_unprotect(self):
        """
        Sets a database object to unprotected

        Args:
            self (object): Database Object

        Returns:
            Return json from the post to assign SLA

        """

        payload = {
            "managedIds": ["{0}".format(self.oracle_id)],
            "existingSnapshotRetention": "RetainSnapshots"
        }

        oracle_db_protect_info = self.rubrik.connection.post('v2', '/sla_domain/UNPROTECTED/assign', payload, timeout=self.cdm_timeout)
        return oracle_db_protect_info


    def oracle_db_protect(self, sla_id, inherit=False):
        """
        Add the database to an SLA Domain Policy or set it to inherit the SLA Domain Policy

        Args:
            self (object): Database Object
            sla_id (str): The Rubrik SLA ID.
            Inherit (bool): Set the database object to inherit it's protection from the host/cluster.

        Returns:
            Return json from the post to assign SLA

        """
        if inherit or not sla_id:
            sla_id = 'INHERIT'

        payload = {
            "managedIds": ["{0}".format(self.oracle_id)],
            "existingSnapshotRetention": "RetainSnapshots",
            "shouldApplyToExistingSnapshots": False,
            "shouldApplyToNonPolicySnapshots": False
        }

        oracle_db_protect_info = self.rubrik.connection.post('v2', '/sla_domain/{0}/assign'.format(sla_id), payload,
                                                              timeout=self.cdm_timeout)
        return oracle_db_protect_info

    @staticmethod
    def match_hostname(hostname1, hostname2):
        """
        Checks 2 hostnames for a match. Will match short name (no domain) to FQDNs.

        Args:
            hostname1: Hostname for comparison
            hostname2: Hostname for comparison

        Returns: True if matched

        """
        for x in range(min(len(hostname1.split('.')), len(hostname2.split('.')))):
            if hostname1.split('.')[x] != hostname2.split('.')[x]:
                return False
        return True


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

    @staticmethod
    def b64_encode(raw_file):
        if type(raw_file) is str:
            raw_file = raw_file.encode("ascii")
        elif type(raw_file) is not bytes:
            self.rubrik.delete_session()
            raise RbsOracleCommonError("Trying to base64 encode a file of the wrong type: {}".format(type(raw_file)))
        base64_file = base64.b64encode(raw_file).decode('ascii')
        return base64_file


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

    def live_mount_delete(self, live_mount_id, force=False):
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


class RubrikRbsRplOracleMount():
    """
    Rubrik RBS (snappable) Oracle backup object.
    """
    def __init__(self, rubrik, database,  target_host):
        self.logger = logging.getLogger(__name__ + '.RubrikRbsOracleMount')
        self.cdm_timeout = 180
        self.rubrik = rubrik
        self.database = database
        self.target_host = target_host

    def get_oracle_live_mount_id(self):
        """
        This will search for and retrieve the live mount id for a live mount of the database on the host.

        Args:
            self (object): Database Object
        Returns:
            live_mount_id (str): The id of the requested live mount.
        """
        oracle_live_mounts = self.rubrik.connection.get('internal', '/oracle/db/mount?source_database_name={}'.format(self.database))
        live_mount_ids = []
        for live_mount in oracle_live_mounts['data']:
            if RubrikRbsOracleDatabase.match_hostname(live_mount['targetHostname'], self.target_host):
                live_mount_ids.append(live_mount['id'])
        if live_mount_ids:
            return live_mount_ids


    def get_live_mount_info(self, live_mount_id):
        """
        Gets all the information about the live mount using the live mount id.
        """
        live_mount_info = self.rubrik.connection.get('internal', '/oracle/db/mount/{}'.format(live_mount_id))
        return live_mount_info

    def live_mount_delete(self, live_mount_id, force=False):
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

    def async_requests_wait(self, requests_id, timeout):
        timeout_start = time.time()
        terminal_states = ['FAILED', 'CANCELED', 'SUCCEEDED']
        oracle_request = None
        while time.time() < timeout_start + (timeout * 60):
            oracle_request = self.rubrik.connection.get('internal', '/oracle/request/{}'.format(requests_id), timeout=self.cdm_timeout)
            if oracle_request['status'] in terminal_states:
                break
            with yaspin(Spinners.line, text='Request status: {}'.format(oracle_request['status'])):
                time.sleep(10)
        if oracle_request['status'] not in terminal_states:
            self.rubrik.delete_session()
            raise RbsOracleCommonError(
                "\nTimeout: Async request status has been {0} for longer than the timeout period of {1} minutes. The request will remain active (current status: {0})  and the script will exit.".format(
                    oracle_request['status'], timeout))
        else:
            return oracle_request


class RubrikRbsOracleHost:
    """
    Rubrik RBS (snappable) Oracle Host object.
    """
    def __init__(self, rubrik, oracle_host, timeout=180):
        self.logger = logging.getLogger(__name__ + '.RubrikRbsOracleHost')
        self.cdm_timeout = timeout
        self.oracle_host = oracle_host
        self.rubrik = rubrik
        self.id = self.get_host_id()

    def get_host_id(self):
        """
        This will search for and retrieve the host id using the hostname.

        Args:
            self (object): Host Object
        Returns:
            host_id (str): The id of the requested live mount.
        """
        self.logger.debug("Getting Id for host: {0}.".format(self.oracle_host))
        hosts = self.rubrik.connection.get('v1', '/host?name={0}'.format(self.oracle_host))
        self.logger.debug(hosts)

        host_id = None
        if hosts['total'] < 1:
            self.logger.debug("No host ID found for host {}...".format(self.oracle_host))
            self.rubrik.delete_session()
            raise RbsOracleCommonError(
                "The host object '{}' was not found on the Rubrik cluster.".format(self.oracle_host))
        elif hosts['total'] == 1:
            host_id = hosts['data'][0]['id']
        elif hosts['total'] > 1:
            self.logger.debug("Multiple host IDs found for host {}...".format(self.oracle_host))
            self.rubrik.delete_session()
            raise RbsOracleCommonError(
                "Multiple host IDs found for '{}' on the Rubrik cluster.".format(self.oracle_host))
        if host_id:
            self.logger.debug(
                "Found Host id: {0} for host: {1}".format(host_id, self.oracle_host))
            return host_id
        else:
            self.rubrik.delete_session()
            raise RbsOracleCommonError(
                "No ID found for a host with name {}.".format(self.oracle_host))


    def refresh(self):
        """
                Refreshes Oracle database host in CDM.

                Args:
                    self (object): Host Object
                Returns:
                    response (str): The response json from the refresh
                """
        oracle_host_refresh_info = self.rubrik.connection.post('v1',
                                                                   '/host/{0}/refresh'.format(self.id),
                                                                   '', timeout=self.cdm_timeout)
        return oracle_host_refresh_info


class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class"""

@dataclass
class Timer:
    timers: ClassVar[Dict[str, float]] = {}
    name: Optional[str] = None
    text: str = "Elapsed time: {:0.4f} seconds"
    logger: Optional[Callable[[str], None]] = print
    _start_time: Optional[float] = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        """Add timer to dict of timers after initialization"""
        if self.name is not None:
            self.timers.setdefault(self.name, 0)

    def start(self) -> None:
        """Start a new timer"""
        if self._start_time is not None:
            self.rubrik.delete_session()
            raise TimerError(f"Timer is running. Use .stop() to stop it")

        self._start_time = time.perf_counter()

    def stop(self) -> float:
        """Stop the timer, and report the elapsed time"""
        if self._start_time is None:
            self.rubrik.delete_session()
            raise TimerError(f"Timer is not running. Use .start() to start it")

        # Calculate elapsed time
        elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None

        # Report elapsed time
        if self.logger:
            self.logger(self.text.format(elapsed_time))
        if self.name:
            self.timers[self.name] += elapsed_time

        return elapsed_time




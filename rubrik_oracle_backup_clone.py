
import rbs_oracle_common
import click
import logging
import sys
import os
import platform
import json
import configparser


@click.command()
@click.option('--source_host_db', '-s', type=str, required=True,  help='The source <host or RAC cluster>:<database>')
@click.option('--mount_path', '-m', type=str, required=True, help='The path used to mount the backup files')
@click.option('--host_target', '-h', type=str, required=True, help='Host or RAC cluster name for the Live Mount.')
@click.option('--new_oracle_name', '-n', type=str, required=True, help='Name for the cloned live mounted database')
@click.option('--configuration', '-c', type=str, help='Oracle duplicate configuration file')
@click.option('--configuration_file', '-f', type=str, help='Oracle duplicate configuration file')
@click.option('--oracle_home', '-o', type=str, help='ORACLE_HOME path for this database clone')
@click.option('--time_restore', '-t', type=str, help='The point in time for the database clone in  iso 8601 format (2019-04-30T18:23:21)')
@click.option('--log_path', '-l', type=str, help='Log directory, if not specified the mount_path with be used.')
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING or CRITICAL.')
def cli(source_host_db, mount_path, time_restore, host_target, oracle_home, new_oracle_name, configuration, configuration_file, log_path, debug_level):
    """
    This will use the Rubrik RMAN backups to do a duplicate (or refresh) of an Oracle Database.

\b
    The source database is specified in a host:db format. The backup mount path is required. If the restore time is not
    provided the most recent recoverable time will be used. The host for the mount clone must be specified. A json or a
    file with a json that includes any configuration changes for the duplicate may be provided. If the Oracle Home is
    not specified the ORACLE_HOME path from the source database will be used. This is for a single instance database
    only, at present it will NOT work on RAC.
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

    # Set up the file logging
    if log_path:
        os.makedirs(log_path, exist_ok=True)
    else:
        log_path = mount_path
    logfile = os.path.join(log_path, "{}_Clone.log".format(new_oracle_name))
    fh = logging.FileHandler(logfile, mode='w')
    fh.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s: %(message)s')
    fh.setFormatter(file_formatter)
    logger.addHandler(fh)

    # Make sure this is being run on the target host
    if host_target.split('.')[0] != platform.uname()[1].split('.')[0]:
        logger.debug("This program must be run on the target host: {}, aborting clone".format(host_target))
        raise RubrikOracleBackupMountCloneError("This program must be run on the target host: {}".format(host_target))
    if len(new_oracle_name) > 8:
        logger.debug("The new oracle name: {} is too long. Oracle names must be 8 characters or less. Aborting clone".format(new_oracle_name))
        raise RubrikOracleBackupMountCloneError("The new oracle name: {} is too long. Oracle names must be 8 characters or less.".format(new_oracle_name))

    # Initialize variables
    spfile = True
    no_file_name_check = False
    refresh_db = False
    drop_database = False

    # Read in the configuration
    if configuration:
        configuration = json.load(configuration)
        logger.debug("Parameters for duplicate loaded from json: {}.".format(configuration))
    elif configuration_file:
        # f = open(configuration_file)
        # configuration = json.load(f)
        configuration = configparser.ConfigParser()
        configuration.read(configuration_file)

        if 'spfile' in configuration['parameters'].keys():
            spfile = configuration['parameters'].getboolean('spfile')
        if 'no_file_name_check' in configuration['parameters'].keys():
            no_file_name_check = configuration['parameters'].getboolean('no_file_name_check')
        if 'refresh_db' in configuration['parameters'].keys():
            refresh_db = configuration['parameters'].getboolean('refresh_db')
        if 'drop_database' in configuration['parameters'].keys():
            drop_database = configuration['parameters'].getboolean('drop_database')
        logger.debug("Parameters for duplicate loaded from file: {}.".format(configuration))


    rubrik = rbs_oracle_common.RubrikConnection()
    source_host_db = source_host_db.split(":")
    database = rbs_oracle_common.RubrikRbsOracleDatabase(rubrik, source_host_db[1], source_host_db[0])
    oracle_db_info = database.get_oracle_db_info()
    # If the source database is on a RAC cluster the target must be a RAC cluster otherwise it will be an Oracle Host
    if 'racName' in oracle_db_info.keys():
        if oracle_db_info['racName']:
            host_id = database.get_rac_id(rubrik.cluster_id, host_target)
    else:
        host_id = database.get_host_id(rubrik.cluster_id, host_target)
    # Use the provided time or if no time has been provided use the the most recent recovery point
    if time_restore:
        time_ms = database.epoch_time(time_restore, rubrik.timezone)
        logger.warning("Using {} for mount.". format(time_restore))
    else:
        logger.warning("Using most recent recovery point for mount.")
        time_ms = database.epoch_time(oracle_db_info['latestRecoveryPoint'], rubrik.timezone)
    # Check ORACLE_HOME and set to source ORACLE_HOME is not provided
    if not oracle_home:
        oracle_home = oracle_db_info['oracleHome']
    if not os.path.exists(oracle_home):
        logger.debug("The ORACLE_HOME: {} does not exist on the target host: {}".format(oracle_home, host_target))
        raise RubrikOracleBackupMountCloneError("The ORACLE_HOME: {} does not exist on the target host: {}".format(oracle_home, host_target))
    # Get directories in path to allow us to find the new directory after the mount
    live_mount_directories = os.listdir(mount_path)
    logger.warning("Starting the mount of the requested {} backup pieces on {}.".format(source_host_db[1], host_target))
    live_mount_info = database.live_mount(host_id, time_ms, files_only=True, mount_path=mount_path)
    live_mount_info = database.async_requests_wait(live_mount_info['id'], 20)
    logger.debug("Backup Live Mount Asyc Request: {}".format(live_mount_info))
    logger.info("Async request completed with status: {}".format(live_mount_info['status']))
    if live_mount_info['status'] != "SUCCEEDED":
        logger.debug("Mount of backup files did not complete successfully. Mount ended with status {}".format(live_mount_info['status']))
        raise RubrikOracleBackupMountCloneError("Mount of backup files did not complete successfully. Mount ended with status {}".format(live_mount_info['status']))
    logger.warning("Live mount of the backup files completed.")
    # Now determine the new live mount directory
    new_live_mount_directories = os.listdir(mount_path)
    live_mount_directory = list(set(new_live_mount_directories) - set(live_mount_directories))
    if len(live_mount_directory) == 1:
        backup_path = os.path.join(mount_path, live_mount_directory[0])
    else:
        logger.debug("Multiple directories were created in {} during this operation. Live mount directory cannot be determined".format(mount_path))
        raise RubrikOracleBackupMountCloneError("Multiple directories were created in {} during this operation. Live mount directory cannot be determined".format(mount_path))
    logger.info("Using the live mount path: {}".format(backup_path))
    live_mount_id = live_mount_directory[0].split('_')[1]
    logger.debug("Live mount ID is {}".format(live_mount_id))

    os.environ["ORACLE_HOME"] = oracle_home
    os.environ["ORACLE_SID"] = new_oracle_name
    logger.debug("Setting env variable ORACLE_HOME={}, ORACLE_SID={}.".format(oracle_home, new_oracle_name))
    if drop_database:
        logger.warning("Dropping Database...")
        logger.info(database.sqlplus_sysdba(oracle_home, 'startup force mount restrict exclusive;'))
        logger.info(database.sqlplus_sysdba(oracle_home, 'drop database;'))
        logger.warning("Database dropped prior to refresh.")
    if spfile:
        init_file = os.path.join(oracle_home, 'dbs', 'init{}.ora'.format(new_oracle_name))
        logger.debug("Creating new temporary init file {}".format(init_file))
        with open(init_file, 'w') as file:
            file.write('db_name={}\n'.format(new_oracle_name))

    logger.warning("Starting auxiliary instance")
    if spfile and not refresh_db:
        sql_return = database.sqlplus_sysdba(oracle_home, "startup nomount pfile='{}'".format(init_file))
        logger.info(sql_return)
    elif spfile and refresh_db:
        logger.info(database.sqlplus_sysdba(oracle_home, "shutdown immediate;"))
        sql_return = database.sqlplus_sysdba(oracle_home, "startup nomount pfile='{}'".format(init_file))
        logger.info(sql_return)
    elif not spfile and refresh_db:
        logger.info(database.sqlplus_sysdba(oracle_home, "shutdown immediate;"))
        sql_return = database.sqlplus_sysdba(oracle_home, "startup nomount")
        logger.info(sql_return)
    else:
        sql_return = database.sqlplus_sysdba(oracle_home, "startup nomount")
        logger.info(sql_return)
    if "ORA-01081: cannot start already-running ORACLE" in sql_return:
        logger.debug("There is an instance of {} all ready running on this host. Aborting clone".format(new_oracle_name))
        raise RubrikOracleBackupMountCloneError("There is an instance of {} all ready running on this host. Aborting clone".format(new_oracle_name))
    sql_return = database.sqlplus_sysdba(oracle_home, "select instance_name from v$instance;")
    logger.info(sql_return)
    if new_oracle_name not in sql_return:
        logger.debug("DB Instance check failed. Instance name is not {}. Aborting clone".format(new_oracle_name))
        raise RubrikOracleBackupMountCloneError("DB Instance check failed. Instance name is not {}. Aborting clone".format(new_oracle_name))

    logger.warning("Beginning duplicate of {} to {} on host {}.".format(source_host_db[1], new_oracle_name, source_host_db[0]))

    duplicate_commands = "duplicate database to {} ".format(new_oracle_name)
    if time_restore:
        time_restore = time_restore.replace("T", "")
        duplicate_commands = duplicate_commands + """until time "TO_DATE('{}','YYYY-MM-DD HH24:MI:SS')"  """.format(time_restore)
    if spfile:
        duplicate_commands = duplicate_commands + "SPFILE parameter_value_convert ('{}','{}') ".format(source_host_db[1], new_oracle_name)
    if configuration['parameters']['db_file_name_convert']:
        duplicate_commands = duplicate_commands + "set  db_file_name_convert = {} ".format(configuration['parameters']['db_file_name_convert'])
    if configuration['parameters']['control_files']:
        duplicate_commands = duplicate_commands + "set  control_files = {} ".format(configuration['parameters']['control_files'])
    if configuration['parameters']['log_file_name_convert']:
        duplicate_commands = duplicate_commands + "set  log_file_name_convert = {} ".format(configuration['parameters']['log_file_name_convert'])

    duplicate_commands = duplicate_commands + "BACKUP LOCATION '{}' ".format(mount_path)
    if no_file_name_check:
        duplicate_commands = duplicate_commands + "NOFILENAMECHECK;"
    else:
        duplicate_commands = duplicate_commands + ";"

    logger.debug("Duplicate script: "
                 "{}".format(duplicate_commands))
    logger.info(database.rman(oracle_home, duplicate_commands, "auxiliary"))
    logger.warning("Duplicate of {} database complete.".format(new_oracle_name))

    mount = rbs_oracle_common.RubrikRbsOracleMount(rubrik, source_host_db[1], source_host_db[0], host_target)
    logger.warning("Unmounting backups.")
    delete_request = mount.live_mount_delete(live_mount_id)
    delete_request = mount.async_requests_wait(delete_request['id'], 12)
    logger.warning("Async request completed with status: {}".format(delete_request['status']))
    logger.debug(delete_request)
    if delete_request['status'] != "SUCCEEDED":
        logger.warning("Unmount of backup files failed with status: {}".format(delete_request['status']))
    else:
        logger.warning("Live mount of backup data files with id: {} has been unmounted.".format(live_mount_id))

    logger.warning("Database clone complete")
    return


class RubrikOracleBackupMountCloneError(rbs_oracle_common.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()

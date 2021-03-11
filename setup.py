from setuptools import setup

setup(
    name='rubrikOracleTools',
    version='1.0',
    py_modules=['rbs_oracle_common', 'rubrik_oracle_backup_info', 'rubrik_oracle_backup_mount',
                'rubrik_oracle_unmount', 'rubrik_oracle_db_mount', 'rubrik_oracle_snapshot',
                'rubrik_oracle_log_backup', 'rubrik_oracle_db_mount_clone', 'rubrik_oracle_clone_unmount',
                'rubrik_oracle_backup_mount_clone', 'rubrik_oracle_mount_info', 'rubrik_oracle_backup_clone',
                'rubrik_oracle_backup_validate'],
    install_requires=[
        'requests >= 2.18.4, != 2.22.0',
        'rubrik_cdm',
        'urllib3 <1.25, >=1.21.1',
        'Click',
        'pytz',
        'yaspin'
    ],
    entry_points='''
        [console_scripts]
        rubrik_oracle_backup_info=rubrik_oracle_backup_info:cli
        rubrik_oracle_backup_mount=rubrik_oracle_backup_mount:cli
        rubrik_oracle_db_mount=rubrik_oracle_db_mount:cli
        rubrik_oracle_unmount=rubrik_oracle_unmount:cli
        rubrik_oracle_snapshot=rubrik_oracle_snapshot:cli
        rubrik_oracle_log_backup=rubrik_oracle_log_backup:cli
        rubrik_oracle_db_mount_clone=rubrik_oracle_db_mount_clone:cli
        rubrik_oracle_clone_unmount=rubrik_oracle_clone_unmount:cli
        rubrik_oracle_backup_mount_clone=rubrik_oracle_backup_mount_clone:cli
        rubrik_oracle_mount_info=rubrik_oracle_mount_info:cli
        rubrik_oracle_backup_clone=rubrik_oracle_backup_clone:cli
        rubrik_oracle_backup_validate=rubrik_oracle_backup_validate:cli
    '''
)
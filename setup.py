from setuptools import setup

setup(
    name='rubrikOracleTools',
    version='1.0',
    py_modules=['oracle_backup_info', 'oracle_mount', 'oracle_unmount'],
    install_requires=[
        'requests',
        'rubrik_cdm',
        'urllib3',
        'Click',
        'pytz',
    ],
    entry_points='''
        [console_scripts]
        oracle_backup_info=oracle_backup_info:cli
        oracle_mount=oracle_mount:cli
        oracle_unmount=oracle_unmount:cli
    '''
)
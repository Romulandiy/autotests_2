import os
import pytest

from aquas.libs.logger import logger
from aquas.libs.test_base import TestBase
from aquas.plugins.hdfs_plugin import HdfsPlugin
from aquas.plugins.hive_plugin import HivePlugin


FILE_DIR = os.path.dirname(__file__)
CONFIG_PATH_DEFAULT = os.path.join(FILE_DIR, 'resources/configs/test_config.yaml')
CONFIG = TestBase.load_yaml_file(CONFIG_PATH_DEFAULT)
DATA_CONF = CONFIG['data']
HIVE_CONF = CONFIG['hive']
HDFS_CONF = CONFIG['hdfs']
PATH_CONF = CONFIG['paths']
KEYTAB = PATH_CONF['keytab']

CONFIG_HIVE_CREATE = os.path.join(FILE_DIR, 'resources/sql_scripts/create/')
CONFIG_HIVE_INSERT = os.path.join(FILE_DIR, 'resources/sql_scripts/insert/')


@pytest.fixture(scope='session')
def hive():
    with HivePlugin(host=HIVE_CONF['host'],
                    port=HIVE_CONF['port'],
                    user=HIVE_CONF['user'],
                    database=HIVE_CONF['database'],
                    kerberos=True,
                    keytab=KEYTAB) as hive_instance:
        yield hive_instance

@pytest.fixture(scope='session')
def hdfs():
    hdfs_instance = HdfsPlugin(host=HDFS_CONF['host'],
                                user=HDFS_CONF['user'],
                                kerberos=True,
                                keytab=KEYTAB)
    yield hdfs_instance

@pytest.fixture(scope='session', autouse=True)
def test_preparation(hive):
    hive.run_query('USE DL_SB')

    for input_table in DATA_CONF['input_hive_table_names']:
        hive.drop_table(input_table)

    for output_table in DATA_CONF['output_hive_table_names']:
        hive.drop_table(output_table)

    res_path_hive_create = os.listdir(CONFIG_HIVE_CREATE)
    for i in res_path_hive_create:
        one_sql_create_file = os.path.join(CONFIG_HIVE_CREATE, i)
        with open(one_sql_create_file, 'r') as fr:
            text_create = fr.read()
            hive.run_query(text_create)

    res_path_hive_insert = os.listdir(CONFIG_HIVE_INSERT)
    for i in res_path_hive_insert:
        one_sql_insert_file = os.path.join(CONFIG_HIVE_INSERT, i)
        with open(one_sql_insert_file, 'r') as fr:
            text_insert = fr.read()
            hive.run_query(text_insert)

@pytest.fixture(scope='session')
def check_hostname():
    hostname = os.environ["HOSTNAME"]
    logger.info(f'Welcome to {hostname}')

    return 'cloud305' if hostname.startswith('cloud-305') else 'another_host'

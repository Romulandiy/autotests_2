import os
import re

from aquas.libs.logger import logger
from aquas.libs.test_base import TestBase

from autotests.helpers import Helpers


FILE_DIR = os.path.dirname(__file__)
CONFIG_PATH_DEFAULT = os.path.join(FILE_DIR, 'resources/configs/test_config.yaml')
CONFIG = TestBase.load_yaml_file(CONFIG_PATH_DEFAULT)
CONFIG_SSH = CONFIG['ssh']
DATA_CONF = CONFIG['data']

# hdfs base path
HDFS_BASE_PATH = '/apps/hive/warehouse/dl_sb.db'

# hive input table names
HIVE_INPUT_TABLE_1 = DATA_CONF['input_hive_table_names'][0]  # test_table_1
HIVE_INPUT_TABLE_6 = DATA_CONF['input_hive_table_names'][1]  # test_table_6
HIVE_INPUT_TABLE_11 = DATA_CONF['input_hive_table_names'][2]  # test_table_11

# hive output table names
HIVE_OUTPUT_TABLE_1 = DATA_CONF['output_hive_table_names'][0]  # test_table_1_result
HIVE_OUTPUT_TABLE_2 = DATA_CONF['output_hive_table_names'][1]  # test_table_2_result
HIVE_OUTPUT_TABLE_3 = DATA_CONF['output_hive_table_names'][2]  # test_table_3_result
HIVE_OUTPUT_TABLE_4 = DATA_CONF['output_hive_table_names'][3]  # test_table_4_result
HIVE_OUTPUT_TABLE_5 = DATA_CONF['output_hive_table_names'][4]  # test_table_5_result
HIVE_OUTPUT_TABLE_7 = DATA_CONF['output_hive_table_names'][5]  # test_table_7_result
HIVE_OUTPUT_TABLE_8 = DATA_CONF['output_hive_table_names'][6]  # test_table_8_result
HIVE_OUTPUT_TABLE_9 = DATA_CONF['output_hive_table_names'][7]  # test_table_9_result
HIVE_OUTPUT_TABLE_10 = DATA_CONF['output_hive_table_names'][8]  # test_table_10_result
HIVE_OUTPUT_TABLE_11 = DATA_CONF['output_hive_table_names'][9]  # test_table_11_result
HIVE_OUTPUT_TABLE_12 = DATA_CONF['output_hive_table_names'][10]  # test_table_12_result
HIVE_OUTPUT_TABLE_13 = DATA_CONF['output_hive_table_names'][11]  # test_table_13_result
HIVE_OUTPUT_TABLE_14 = DATA_CONF['output_hive_table_names'][12]  # test_table_14_result

# path to configs_test folder
CONFIGS_TEST_PATH = os.path.join(FILE_DIR, 'resources/configs/configs_test/')
CONFIGS_TEST_PATH_LOGS = os.path.join(CONFIGS_TEST_PATH, 'test_logs')

# label for save logs folder
TEST_1 = 'test_1.txt'
TEST_7 = 'test_7.txt'
TEST_14 = 'test_14.txt'

# yaml file from configs_test
BASE_FRAMEWORK_YAML = CONFIG['configs_test'][0]
PARTITION_NUM_1_YAML = CONFIG['configs_test'][1]
PARTITION_NUM_2_YAML = CONFIG['configs_test'][2]
WITHOUT_PARTITION_NUM_YAML = CONFIG['configs_test'][3]
SOURCE_QUERY_RESULT_YAML = CONFIG['configs_test'][4]
DEPENDENCY_YAML = CONFIG['configs_test'][5]
TASK_PARAM_PARTITIONEOM_YAML = CONFIG['configs_test'][6]
TASK_PARAM_PARTITIONBOM_YAML = CONFIG['configs_test'][7]
TASK_PARAM_PARTITIONDAY_START_YAML = CONFIG['configs_test'][8]
TASK_PARAM_PARTITIONDAY_END_YAML = CONFIG['configs_test'][9]
PROCESS_MODE_YAML = CONFIG['configs_test'][10]
TABLE_PARTITION_YAML = CONFIG['configs_test'][11]
VERSION_NUM_HDFS_HIVE_YAML = CONFIG['configs_test'][12]
MDS_HWM_YAML = CONFIG['configs_test'][13]


class Test:

    def test_check_base_framework_with_real_tables(self, hive, hdfs, check_hostname):
        logger.info('START BASE FRAMEWORK TEST WITH REAL TABLES')

        hive_output_table_1_path = os.path.join(HDFS_BASE_PATH, HIVE_OUTPUT_TABLE_1)
        hdfs.delete_hdfs_files(hive_output_table_1_path, recursive=True)

        columns_input_table = ['id', 'color', 'week', 'business_dt']
        columns_dest_table = ['id', 'color', 'week', 'regid', 'business_dt']

        Helpers.run_command_by_host(check_hostname=check_hostname,
                                    configs_test_path=CONFIGS_TEST_PATH,
                                    test_yaml=BASE_FRAMEWORK_YAML,
                                    test_path_logs=CONFIGS_TEST_PATH_LOGS,
                                    label=TEST_1,
                                    **CONFIG_SSH)

        df_from_hive_input = hive.select_all(table_name=HIVE_INPUT_TABLE_1, drop_table_names=True)
        df_from_hive_output = hive.select_all(table_name=HIVE_OUTPUT_TABLE_1, drop_table_names=True)

        actual_input_hive_columns = list(df_from_hive_input.keys().values)
        actual_dest_hive_columns = list(df_from_hive_output.keys().values)

        expected_sentence = 'Dataframe is empty, please check datasource or query'
        search_status = False
        test_1_txt_path = os.path.join(CONFIGS_TEST_PATH_LOGS, TEST_1)
        with open(test_1_txt_path, 'r') as fr:
            search_status = Helpers.condition_check_exception(expected_sentence, fr, search_status)

        assert actual_dest_hive_columns == columns_dest_table, 'Error: fields of the dest table do not match'
        assert actual_input_hive_columns == columns_input_table, 'Error: fields of the source table do not match'
        assert search_status, f'Error: there is no expression in the logs: {expected_sentence}'

    def test_check_mds_hwm(self, hive, hdfs, check_hostname):
        logger.info('START TEST CHECK MDS HWM')

        hive_output_table_14_path = os.path.join(HDFS_BASE_PATH, HIVE_OUTPUT_TABLE_14)
        hdfs.delete_hdfs_files(hive_output_table_14_path, recursive=True)

        Helpers.run_command_by_host(check_hostname=check_hostname,
                                    configs_test_path=CONFIGS_TEST_PATH,
                                    test_yaml=MDS_HWM_YAML,
                                    test_path_logs=CONFIGS_TEST_PATH_LOGS,
                                    label=TEST_14,
                                    **CONFIG_SSH)

        actual_count_rows_of_dest_table = hive.run_query(query=f'SELECT COUNT(business_dt) FROM {HIVE_OUTPUT_TABLE_14}')

        search_sentences = ['The latest HWM(table=dl_sb.test__table_14, column=valid_from_dttm|sibcrm_msk|max) is 2008-12-03',
                             'Check mds_hwm dl_sb.test__table_14 partition(valid_from_dttm|sibcrm_msk|max=2008-12-03): true',
                             'System.exit code: Success']
        test_14_txt_path = os.path.join(CONFIGS_TEST_PATH_LOGS, TEST_14)
        with open(test_14_txt_path, 'r') as fr:
            excluded_sentences = Helpers.condition_check(search_sentences, fr)

        assert excluded_sentences.isdisjoint(search_sentences), f'Error: there is no expression in the logs: {excluded_sentences}'
        assert 14 == actual_count_rows_of_dest_table[0][0], 'Error: count "business_dt" do not match'

    def test_check_forwarding_sources_into_query(self, hive, hdfs, check_hostname):
        logger.info('START TEST FORWARDING SOURCES INTO QUERY')

        hive_output_table_2_path = os.path.join(HDFS_BASE_PATH, HIVE_OUTPUT_TABLE_2)
        hdfs.delete_hdfs_files(hive_output_table_2_path, recursive=True)

        columns_dest_table = ['table_name_col1', 'ssc_col2', 'col_regid', 'part_dt', 'business_dt']
        values_of_first_row_dest_table = ['dl_sb.test__table_50', 'siebel_50', '50', '2021-03-10', '2021-01-24']

        Helpers.run_command_by_host(check_hostname=check_hostname,
                                    configs_test_path=CONFIGS_TEST_PATH,
                                    test_yaml=PARTITION_NUM_1_YAML,
                                    **CONFIG_SSH)

        df_from_hive_output = hive.select_all(table_name=HIVE_OUTPUT_TABLE_2, drop_table_names=True)
        actual_dest_columns = list(df_from_hive_output.keys().values)
        actual_dest_values_of_first_row = list(df_from_hive_output.values[0])

        assert actual_dest_columns == columns_dest_table, 'Error: different names of field in tables'
        assert actual_dest_values_of_first_row == values_of_first_row_dest_table, 'Error: field values do not match'

    def test_check_partition_num_value_1(self, hdfs, check_hostname):
        logger.info('START TEST CHECK PARTITION_NUM WITH VALUE = 1')

        hive_output_table_2_path = os.path.join(HDFS_BASE_PATH, HIVE_OUTPUT_TABLE_2)
        hdfs.delete_hdfs_files(hive_output_table_2_path, recursive=True)

        Helpers.run_command_by_host(check_hostname=check_hostname,
                                    configs_test_path=CONFIGS_TEST_PATH,
                                    test_yaml=PARTITION_NUM_1_YAML,
                                    **CONFIG_SSH)

        expected_result_part_1 = [f'test__table_2_result/part_dt=2021-03-10/business_dt=2021-01-24/part-']
        expected_result_part_2 = [f'test__table_2_result/part_dt=2021-03-10/business_dt=2021-01-25/part-']
        expected_result_part_3 = [f'test__table_2_result/part_dt=2021-03-15/business_dt=2021-01-23/part-']
        expected_result_part_4 = [f'test__table_2_result/part_dt=2021-03-19/business_dt=2021-01-26/part-']

        hdfs_files_part_1: list = hdfs.get_hdfs_files(hdfs_path=f'/apps/hive/warehouse/dl_sb.db/test__table_2_result/part_dt=2021-03-10/business_dt=2021-01-24')
        hdfs_files_part_2: list = hdfs.get_hdfs_files(hdfs_path=f'/apps/hive/warehouse/dl_sb.db/test__table_2_result/part_dt=2021-03-10/business_dt=2021-01-25')
        hdfs_files_part_3: list = hdfs.get_hdfs_files(hdfs_path=f'/apps/hive/warehouse/dl_sb.db/test__table_2_result/part_dt=2021-03-15/business_dt=2021-01-23')
        hdfs_files_part_4: list = hdfs.get_hdfs_files(hdfs_path=f'/apps/hive/warehouse/dl_sb.db/test__table_2_result/part_dt=2021-03-19/business_dt=2021-01-26')

        regular_expression = r'test\w*\/\w*=\d{4}-\d{2}-\d{2}\/\w*=\d{4}-\d{2}-\d{2}\/part-'
        actual_result_part_1 = [re.search(regular_expression, path_part_1).group() for path_part_1 in hdfs_files_part_1]

        logger.info(f'actual_result_part_1 = {actual_result_part_1}')
        logger.info(f'expected_result_part_1 = {expected_result_part_1}')

        actual_result_part_2 = [re.search(regular_expression, path_part_2).group() for path_part_2 in hdfs_files_part_2]

        logger.info(f'actual_result_part_2 = {actual_result_part_2}')
        logger.info(f'expected_result_part_2 = {expected_result_part_2}')

        actual_result_part_3 = [re.search(regular_expression, path_part_3).group() for path_part_3 in hdfs_files_part_3]

        logger.info(f'actual_result_part_3 = {actual_result_part_3}')
        logger.info(f'expected_result_part_3 = {expected_result_part_3}')

        actual_result_part_4 = [re.search(regular_expression, path_part_4).group() for path_part_4 in hdfs_files_part_4]

        logger.info(f'actual_result_part_4 = {actual_result_part_4}')
        logger.info(f'expected_result_part_4 = {expected_result_part_4}')

        assert expected_result_part_1 == actual_result_part_1, 'Error: condition "partition_num: 1" is broke. Do not match hdfs file of part_dt=2021-03-10/business_dt=2021-01-24/'
        assert expected_result_part_2 == actual_result_part_2, 'Error: condition "partition_num: 1" is broke. Do not match hdfs file of part_dt=2021-03-10/business_dt=2021-01-25/'
        assert expected_result_part_3 == actual_result_part_3, 'Error: condition "partition_num: 1" is broke. Do not match hdfs file of part_dt=2021-03-15/business_dt=2021-01-23/'
        assert expected_result_part_4 == actual_result_part_4, 'Error: condition "partition_num: 1" is broke. Do not match hdfs file of part_dt=2021-03-19/business_dt=2021-01-26/'

    def test_check_partition_num_value_2(self, hdfs, check_hostname):
        logger.info('START TEST CHECK PARTITION_NUM WITH VALUE = 2')

        hive_output_table_3_path = os.path.join(HDFS_BASE_PATH, HIVE_OUTPUT_TABLE_3)
        hdfs.delete_hdfs_files(hive_output_table_3_path, recursive=True)

        Helpers.run_command_by_host(check_hostname=check_hostname,
                                    configs_test_path=CONFIGS_TEST_PATH,
                                    test_yaml=PARTITION_NUM_2_YAML,
                                    **CONFIG_SSH)

        expected_result_part_1 = [f'test__table_3_result/part_dt=2021-03-10/business_dt=2021-01-24/part-',
                                  f'test__table_3_result/part_dt=2021-03-10/business_dt=2021-01-24/part-']
        expected_result_part_2 = [f'test__table_3_result/part_dt=2021-03-10/business_dt=2021-01-25/part-',
                                  f'test__table_3_result/part_dt=2021-03-10/business_dt=2021-01-25/part-']
        expected_result_part_3 = [f'test__table_3_result/part_dt=2021-03-15/business_dt=2021-01-23/part-',
                                  f'test__table_3_result/part_dt=2021-03-15/business_dt=2021-01-23/part-']
        expected_result_part_4 = [f'test__table_3_result/part_dt=2021-03-19/business_dt=2021-01-26/part-',
                                  f'test__table_3_result/part_dt=2021-03-19/business_dt=2021-01-26/part-']

        hdfs_files_part_1: list = hdfs.get_hdfs_files(
            hdfs_path=f'/apps/hive/warehouse/dl_sb.db/test__table_3_result/part_dt=2021-03-10/business_dt=2021-01-24')
        hdfs_files_part_2: list = hdfs.get_hdfs_files(
            hdfs_path=f'/apps/hive/warehouse/dl_sb.db/test__table_3_result/part_dt=2021-03-10/business_dt=2021-01-25')
        hdfs_files_part_3: list = hdfs.get_hdfs_files(
            hdfs_path=f'/apps/hive/warehouse/dl_sb.db/test__table_3_result/part_dt=2021-03-15/business_dt=2021-01-23')
        hdfs_files_part_4: list = hdfs.get_hdfs_files(
            hdfs_path=f'/apps/hive/warehouse/dl_sb.db/test__table_3_result/part_dt=2021-03-19/business_dt=2021-01-26')

        regular_expression = r'test\w*\/\w*=\d{4}-\d{2}-\d{2}\/\w*=\d{4}-\d{2}-\d{2}\/part-'
        actual_result_part_1 = [re.search(regular_expression, path_part_1).group() for path_part_1 in hdfs_files_part_1]

        logger.info(f'actual_result_part_1 = {actual_result_part_1}')
        logger.info(f'expected_result_part_1 = {expected_result_part_1}')

        actual_result_part_2 = [re.search(regular_expression, path_part_2).group() for path_part_2 in hdfs_files_part_2]

        logger.info(f'actual_result_part_2 = {actual_result_part_2}')
        logger.info(f'expected_result_part_2 = {expected_result_part_2}')

        actual_result_part_3 = [re.search(regular_expression, path_part_3).group() for path_part_3 in hdfs_files_part_3]

        logger.info(f'actual_result_part_3 = {actual_result_part_3}')
        logger.info(f'expected_result_part_3 = {expected_result_part_3}')

        actual_result_part_4 = [re.search(regular_expression, path_part_4).group() for path_part_4 in hdfs_files_part_4]

        logger.info(f'actual_result_part_4 = {actual_result_part_4}')
        logger.info(f'expected_result_part_4 = {expected_result_part_4}')

        assert expected_result_part_1 == actual_result_part_1, 'Error: condition "partition_num: 2" is broke. Do not match hdfs file of part_dt=2021-03-10/business_dt=2021-01-24/'
        assert expected_result_part_2 == actual_result_part_2, 'Error: condition "partition_num: 2" is broke. Do not match hdfs file of part_dt=2021-03-10/business_dt=2021-01-25/'
        assert expected_result_part_3 == actual_result_part_3, 'Error: condition "partition_num: 2" is broke. Do not match hdfs file of part_dt=2021-03-15/business_dt=2021-01-23/'
        assert expected_result_part_4 == actual_result_part_4, 'Error: condition "partition_num: 2" is broke. Do not match hdfs file of part_dt=2021-03-19/business_dt=2021-01-26/'

    def test_check_without_partition_num(self, hdfs, check_hostname):
        logger.info('START TEST WITHOUT PARTITION_NUM')

        hive_output_table_4_path = os.path.join(HDFS_BASE_PATH, HIVE_OUTPUT_TABLE_4)
        hdfs.delete_hdfs_files(hive_output_table_4_path, recursive=True)

        Helpers.run_command_by_host(check_hostname=check_hostname,
                                    configs_test_path=CONFIGS_TEST_PATH,
                                    test_yaml=WITHOUT_PARTITION_NUM_YAML,
                                    **CONFIG_SSH)

        expected_result_part_1 = [f'test__table_4_result/part_dt=2021-03-10/business_dt=2021-01-24/part-',
                                  f'test__table_4_result/part_dt=2021-03-10/business_dt=2021-01-24/part-']
        expected_result_part_2 = [f'test__table_4_result/part_dt=2021-03-10/business_dt=2021-01-25/part-',
                                  f'test__table_4_result/part_dt=2021-03-10/business_dt=2021-01-25/part-',
                                  f'test__table_4_result/part_dt=2021-03-10/business_dt=2021-01-25/part-',
                                  f'test__table_4_result/part_dt=2021-03-10/business_dt=2021-01-25/part-']
        expected_result_part_3 = [f'test__table_4_result/part_dt=2021-03-15/business_dt=2021-01-23/part-',
                                  f'test__table_4_result/part_dt=2021-03-15/business_dt=2021-01-23/part-']
        expected_result_part_4 = [f'test__table_4_result/part_dt=2021-03-19/business_dt=2021-01-26/part-',
                                  f'test__table_4_result/part_dt=2021-03-19/business_dt=2021-01-26/part-']

        hdfs_files_part_1: list = hdfs.get_hdfs_files(
            hdfs_path=f'/apps/hive/warehouse/dl_sb.db/test__table_4_result/part_dt=2021-03-10/business_dt=2021-01-24')
        hdfs_files_part_2: list = hdfs.get_hdfs_files(
            hdfs_path=f'/apps/hive/warehouse/dl_sb.db/test__table_4_result/part_dt=2021-03-10/business_dt=2021-01-25')
        hdfs_files_part_3: list = hdfs.get_hdfs_files(
            hdfs_path=f'/apps/hive/warehouse/dl_sb.db/test__table_4_result/part_dt=2021-03-15/business_dt=2021-01-23')
        hdfs_files_part_4: list = hdfs.get_hdfs_files(
            hdfs_path=f'/apps/hive/warehouse/dl_sb.db/test__table_4_result/part_dt=2021-03-19/business_dt=2021-01-26')

        regular_expression = r'test\w*\/\w*=\d{4}-\d{2}-\d{2}\/\w*=\d{4}-\d{2}-\d{2}\/part-'
        actual_result_part_1 = [re.search(regular_expression, path_part_1).group() for path_part_1 in hdfs_files_part_1]

        logger.info(f'actual_result_part_1 = {actual_result_part_1}')
        logger.info(f'expected_result_part_1 = {expected_result_part_1}')

        actual_result_part_2 = [re.search(regular_expression, path_part_2).group() for path_part_2 in hdfs_files_part_2]

        logger.info(f'actual_result_part_2 = {actual_result_part_2}')
        logger.info(f'expected_result_part_2 = {expected_result_part_2}')

        actual_result_part_3 = [re.search(regular_expression, path_part_3).group() for path_part_3 in hdfs_files_part_3]

        logger.info(f'actual_result_part_3 = {actual_result_part_3}')
        logger.info(f'expected_result_part_3 = {expected_result_part_3}')

        actual_result_part_4 = [re.search(regular_expression, path_part_4).group() for path_part_4 in hdfs_files_part_4]

        logger.info(f'actual_result_part_4 = {actual_result_part_4}')
        logger.info(f'expected_result_part_4 = {expected_result_part_4}')

        assert expected_result_part_1 == actual_result_part_1, 'Error: condition without "partition_num:" is broke. Do not match hdfs file of part_dt=2021-03-10/business_dt=2021-01-24/'
        assert expected_result_part_2 == actual_result_part_2, 'Error: condition without "partition_num:" is broke. Do not match hdfs file of part_dt=2021-03-10/business_dt=2021-01-25/'
        assert expected_result_part_3 == actual_result_part_3, 'Error: condition without "partition_num:" is broke. Do not match hdfs file of part_dt=2021-03-15/business_dt=2021-01-23/'
        assert expected_result_part_4 == actual_result_part_4, 'Error: condition without "partition_num:" is broke. Do not match hdfs file of part_dt=2021-03-19/business_dt=2021-01-26/'

    def test_check_source_query_result(self, hive, hdfs, check_hostname):
        logger.info('START TEST CHECK FORWARDING FROM SOURCE_QUERY INTO COMBINE_QUERY BY SOURCE_QUERY_RESULT')

        hive_output_table_5_path = os.path.join(HDFS_BASE_PATH, HIVE_OUTPUT_TABLE_5)
        hdfs.delete_hdfs_files(hive_output_table_5_path, recursive=True)

        Helpers.run_command_by_host(check_hostname=check_hostname,
                                    configs_test_path=CONFIGS_TEST_PATH,
                                    test_yaml=SOURCE_QUERY_RESULT_YAML,
                                    **CONFIG_SSH)

        actual_df_count_rows_after_calculation = hive.select_all(table_name=HIVE_OUTPUT_TABLE_5, drop_table_names=True, columns=['count_color'])

        assert 3 == actual_df_count_rows_after_calculation.values[0][0], 'Error: condition forwarding from source_query into combine_query by "source_query_result" is broke'

    def test_check_dependency(self, hive, hdfs, check_hostname):
        logger.info('START TEST CHECK DEPENDENCY')

        hive_output_table_7_path = os.path.join(HDFS_BASE_PATH, HIVE_OUTPUT_TABLE_7)
        hdfs.delete_hdfs_files(hive_output_table_7_path, recursive=True)

        Helpers.run_command_by_host(check_hostname=check_hostname,
                                    configs_test_path=CONFIGS_TEST_PATH,
                                    test_yaml=DEPENDENCY_YAML,
                                    test_path_logs=CONFIGS_TEST_PATH_LOGS,
                                    label=TEST_7,
                                    **CONFIG_SSH)

        actual_df_after_calculation = hive.select_all(table_name=HIVE_OUTPUT_TABLE_7, drop_table_names=True, columns=['COUNT(part_dt) as count_part_dt'])

        logger.info(f'actual_df_after_calculation = {actual_df_after_calculation}')

        expected_sentence = 'System.exit code: DependencyIsNotReady'
        search_status = False
        test_7_txt_path = os.path.join(CONFIGS_TEST_PATH_LOGS, TEST_7)
        with open(test_7_txt_path, 'r') as fr:
            search_status = Helpers.condition_check_exception(expected_sentence, fr, search_status)

        assert 5 == actual_df_after_calculation.values[0][0], 'Error: condition dependency "partition_exist: .part_dt=[0]" is broke. Count "part_dt" do not match'
        assert search_status, 'Error: condition dependency "partition_exist: .part_dt=[0]" is broke. Do not have expression "System.exit code: DependencyIsNotReady"'

    def test_check_task_param_partitioneom(self, hive, hdfs, check_hostname):
        logger.info('START TEST CHECK TASK_PARAM WITH PartitionEom')

        hive_output_table_8_path = os.path.join(HDFS_BASE_PATH, HIVE_OUTPUT_TABLE_8)
        hdfs.delete_hdfs_files(hive_output_table_8_path, recursive=True)
        expected_value_part_dt: list = Helpers.expected_result_partition_eom_bom(special_day='special_2')

        logger.info(f'expected_value_part_dt = {expected_value_part_dt}')

        if not expected_value_part_dt or expected_value_part_dt is None:
            raise RuntimeError('My exception! Please check and update date (part_dt) in "task_param_test.yaml" \n'
                               'to actual dates (for example: change year to new) and then restart test-cases.')

        Helpers.run_command_by_host(check_hostname=check_hostname,
                                    configs_test_path=CONFIGS_TEST_PATH,
                                    test_yaml=TASK_PARAM_PARTITIONEOM_YAML,
                                    **CONFIG_SSH)

        actual_df_after_calculation = hive.select_all(table_name=HIVE_OUTPUT_TABLE_8, drop_table_names=True, columns=['part_dt'])
        actual_value_part_dt = [value[0] for value in actual_df_after_calculation.values]

        logger.info(f'actual_value_part_dt = {actual_value_part_dt}')

        assert actual_value_part_dt == expected_value_part_dt, 'Error: condition "task_param: param_class: test.it.params.PartitionEom" is broke'

    def test_check_task_param_partitionbom(self, hive, hdfs, check_hostname):
        logger.info('START TEST CHECK TASK_PARAM WITH PartitionBom')

        hive_output_table_9_path = os.path.join(HDFS_BASE_PATH, HIVE_OUTPUT_TABLE_9)
        hdfs.delete_hdfs_files(hive_output_table_9_path, recursive=True)
        expected_value_part_dt: list = Helpers.expected_result_partition_eom_bom(special_day='special')

        logger.info(f'expected_value_part_dt = {expected_value_part_dt}')

        if not expected_value_part_dt or expected_value_part_dt is None:
            raise RuntimeError('My exception! Please check and update date (part_dt) in "task_param_test.yaml" \n'
                               'to actual dates (for example: change year to new) and then restart test-cases.')

        Helpers.run_command_by_host(check_hostname=check_hostname,
                                    configs_test_path=CONFIGS_TEST_PATH,
                                    test_yaml=TASK_PARAM_PARTITIONBOM_YAML,
                                    **CONFIG_SSH)

        actual_df_after_calculation = hive.select_all(table_name=HIVE_OUTPUT_TABLE_9, drop_table_names=True, columns=['part_dt'])
        actual_value_part_dt: list = [value[0] for value in actual_df_after_calculation.values]

        logger.info(f'actual_value_part_dt = {actual_value_part_dt}')

        assert actual_value_part_dt == expected_value_part_dt, 'Error: condition "task_param: param_class: test.it.params.PartitionBom" is broke'

    def test_check_task_param_partitionday(self, hdfs, check_hostname):
        logger.info('START TEST CHECK TASK_PARAM WITH PartitionDay')

        hive_output_table_10_path = os.path.join(HDFS_BASE_PATH, HIVE_OUTPUT_TABLE_10)
        hdfs.delete_hdfs_files(hive_output_table_10_path, recursive=True)

        Helpers.run_command_by_host(check_hostname=check_hostname,
                                    configs_test_path=CONFIGS_TEST_PATH,
                                    test_yaml=TASK_PARAM_PARTITIONDAY_START_YAML,
                                    **CONFIG_SSH)

        hdfs.delete_hdfs_files(hive_output_table_10_path, recursive=True)

        Helpers.run_command_by_host(check_hostname=check_hostname,
                                    configs_test_path=CONFIGS_TEST_PATH,
                                    test_yaml=TASK_PARAM_PARTITIONDAY_END_YAML,
                                    **CONFIG_SSH)

        expected_business_dt = ['2021-01-29', '2021-01-30', '2021-01-31']
        expected_result = [f'test__table_10_result/business_dt={one_business_dt}' for one_business_dt in expected_business_dt]

        logger.info(f'expected_result = {expected_result}')

        hdfs_files_part: list = hdfs.get_hdfs_files(hdfs_path=os.path.join(HDFS_BASE_PATH, HIVE_OUTPUT_TABLE_10))

        logger.info(f'hdfs_files_part = {hdfs_files_part}')

        regular_expression = r'test\w*\/\w*=\d{4}-\d{2}-\d{2}'
        actual_result = [re.search(regular_expression, file_part).group() for file_part in hdfs_files_part]

        logger.info(f'actual_result= {actual_result}')

        assert actual_result == expected_result, 'Error: condition "task_param: param_class: test.it.params.PartitionDay" with "overwrite_partitions" is broke'

    def test_check_process_mode(self, hdfs, hive, check_hostname):
        logger.info('START TEST CHECK PROCESS_MODE: \"CREATE_TABLE_IF_NOT_EXISTS\"')

        hive_output_table_11_path = os.path.join(HDFS_BASE_PATH, HIVE_OUTPUT_TABLE_11)
        hdfs.delete_hdfs_files(hive_output_table_11_path, recursive=True)

        Helpers.run_command_by_host(check_hostname=check_hostname,
                                    configs_test_path=CONFIGS_TEST_PATH,
                                    test_yaml=PROCESS_MODE_YAML,
                                    **CONFIG_SSH)

        try:
            df = hive.describe_table(table_name='dl_sb.test__table_11_result')
        except Exception as exc:
            statement_exception = r'FAILED: SemanticException [Error 10001]: Table not found dl_sb.test__table_11_result'
            if statement_exception in str(exc):
                raise RuntimeError('My exception! Please check attributes of "process_mode_test.yaml" probably dest table not exist')

        assert 'value' == df.values[0][0], 'Error: condition "process_mode" with value "CREATE_TABLE_IF_NOT_EXISTS" is broke'

    def test_check_tablepartition(self, hive, hdfs, check_hostname):
        logger.info('START TEST CHECK transform_class: test.it.dds.TablePartition')

        expected_result = ['value', 'part_dt']

        hive_output_table_12_path = os.path.join(HDFS_BASE_PATH, HIVE_OUTPUT_TABLE_12)
        hdfs.delete_hdfs_files(hive_output_table_12_path, recursive=True)

        Helpers.run_command_by_host(check_hostname=check_hostname,
                                    configs_test_path=CONFIGS_TEST_PATH,
                                    test_yaml=TABLE_PARTITION_YAML,
                                    **CONFIG_SSH)

        actual_df_after_calculation = hive.select_all(table_name=HIVE_OUTPUT_TABLE_12, drop_table_names=True)
        actual_result = [column_name for column_name in actual_df_after_calculation.keys()]

        logger.info(f'actual_result = {actual_result}')

        assert expected_result == actual_result, 'Error: condition "TablePartition" is broke'

    def test_check_version_num_hdfs_hive(self, hive, hdfs, check_hostname):
        logger.info('START TEST CHECK VERSION_NUM_HDFS AND VERSION_NUM_HIVE')

        hive_output_table_13_path = os.path.join(HDFS_BASE_PATH, HIVE_OUTPUT_TABLE_13)
        hdfs.delete_hdfs_files(hive_output_table_13_path, recursive=True)

        Helpers.run_command_by_host(check_hostname=check_hostname,
                                    configs_test_path=CONFIGS_TEST_PATH,
                                    test_yaml=VERSION_NUM_HDFS_HIVE_YAML,
                                    **CONFIG_SSH)

        expected_result_part_1 = f'test__table_13_result/part_dt=2021-03-15/part-'
        expected_result_part_2 = f'test__table_13_result/part_dt=2021-03-19/part-'

        hdfs_files_part_1: list = hdfs.get_hdfs_files(hdfs_path=f'/apps/hive/warehouse/dl_sb.db/test__table_13_result/part_dt=2021-03-15')
        hdfs_files_part_2: list = hdfs.get_hdfs_files(hdfs_path=f'/apps/hive/warehouse/dl_sb.db/test__table_13_result/part_dt=2021-03-19')

        regular_expression = r'test\w*\/\w*=\d{4}-\d{2}-\d{2}\/part-'
        actual_result_part_1 = re.search(regular_expression, str(hdfs_files_part_1)).group()

        logger.info(f'actual_result_part_1 = {actual_result_part_1}')
        logger.info(f'expected_result_part_1 = {expected_result_part_1}')

        actual_result_part_2 = re.search(regular_expression, str(hdfs_files_part_2)).group()

        logger.info(f'actual_result_part_2 = {actual_result_part_2}')
        logger.info(f'expected_result_part_2 = {expected_result_part_2}')

        expected_show_partitons_hive = 'part_dt=2021-03-19'
        actual_show_partitons_hive: list(tuple) = hive.run_query(query=f'SHOW PARTITIONS {CONFIG["hive"]["database"]}.{HIVE_OUTPUT_TABLE_13}')

        logger.info(f'actual_show_partitons_hive = {actual_show_partitons_hive}')

        assert expected_result_part_1 == actual_result_part_1, 'Error: condition "version_num_hdfs: 2" is broke. Do not match hdfs file of part_dt=2021-03-15'
        assert expected_result_part_2 == actual_result_part_2, 'Error: condition "version_num_hdfs: 2" is broke. Do not match hdfs file of part_dt=2021-03-19'
        assert expected_show_partitons_hive == actual_show_partitons_hive[0][0], 'Error: condition "version_num_hive: 1" is broke. Do not match hive partitons'

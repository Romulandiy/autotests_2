import os
import re
import calendar
import shutil
import paramiko

from datetime import date, datetime

from aquas.libs.logger import logger


class Helpers:

    @staticmethod
    def run_command_cloud_via_ssh(configs_test_path, test_yaml, test_path_logs, label, **ssh_args):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=ssh_args['host'], port=ssh_args['port'], username=ssh_args['user'], password=ssh_args['password'])
        out_list = []
        err_list = []

        with ssh.open_sftp() as sftp:
            stdin, stdout, stderr = ssh.exec_command(f"{'sudo rm -rf /data/test/config/configs_test/'}")
            stdout.channel.recv_exit_status()

            stdin, stdout, stderr = ssh.exec_command(f"{'sudo mkdir -m0777 /data/test/config/configs_test/'}")
            stdout.channel.recv_exit_status()

            logger.info('run_command_cloud_via_ssh')

            if label is None:
                localpath = os.path.join(configs_test_path, f'{test_yaml}.yaml')
                logger.info(f'localpath = {localpath}')
                remotepath = f'/data/test/config/configs_test/{test_yaml}.yaml'
                logger.info(f'remotepath = {remotepath}')
                sftp.put(localpath, remotepath)

                tmp_command = f'sudo ./test.sh -t /data/test/config/configs_test/{test_yaml}.yaml'
                stdin, stdout, stderr = ssh.exec_command(f"{'cd /data/test/bin/'} && {tmp_command}")
                stdout.channel.recv_exit_status()

            else:
                stdin, stdout, stderr = ssh.exec_command(f"{'sudo mkdir -m0777 /data/test/config/configs_test/test_logs/'}")
                stdout.channel.recv_exit_status()

                localpath = os.path.join(configs_test_path, f'{test_yaml}.yaml')
                logger.info(f'localpath = {localpath}')
                remotepath = f'/data/test/config/configs_test/{test_yaml}.yaml'
                logger.info(f'remotepath = {remotepath}')
                sftp.put(localpath, remotepath)

                remotepath_logs = os.path.join('/data/test/config/configs_test/test_logs', label)
                localpath_logs = os.path.join(test_path_logs, label)

                if label != 'test_14.txt':
                    tmp_command = f'sudo ./test.sh -t /data/test/config/configs_test/{test_yaml}.yaml 2>&1 | tee -a {remotepath_logs}'
                else:
                    tmp_command = f'sudo ./test.sh -t /data/test/config/configs_test/{test_yaml}.yaml -bd 2008-12-03 2>&1 | tee -a {remotepath_logs}'

                stdin, stdout, stderr = ssh.exec_command(f"{'cd /data/test/bin/'} && {tmp_command}")
                stdout.channel.recv_exit_status()

                sftp.get(remotepath_logs, localpath_logs)

        output = stdout.readlines()
        logger.info(output)
        out_list.append(output)
        errors = stderr.readlines()
        for line in errors:
            logger.info(line)
            err_list.append(errors)
        ssh.close()
        logger.info(f'out_list = {out_list}')
        logger.info(f'err_list = {err_list}')

    @staticmethod
    def run_command_cloud(configs_test_path, test_yaml, test_path_logs, label):
        os.system(f"{'sudo rm -rf /data/test/config/configs_test/'}")
        os.system(f"{'sudo mkdir -m0777 /data/test/config/configs_test/'}")

        localpath = os.path.join(configs_test_path, f'{test_yaml}.yaml')
        logger.info(f'localpath = {localpath}')
        remotepath = f'/data/test/config/configs_test/{test_yaml}.yaml'
        logger.info(f'remotepath = {remotepath}')
        shutil.copy2(localpath, remotepath)

        if label is None:
            tmp_command = f'sudo ./test.sh -t /data/test/config/configs_test/{test_yaml}.yaml'
            os.system(f"{'cd /data/test/bin/'} && {tmp_command}")
        else:
            os.system(f"{'sudo rm -rf /data/test/config/configs_test/test_logs/'}")
            os.system(f"{'sudo mkdir -m0777 /data/test/config/configs_test/test_logs/'}")
            remotepath_logs = os.path.join('/data/test/config/configs_test/test_logs', label)

            if label != 'test_14.txt':
                tmp_command = f'sudo ./test.sh -t /data/test/config/configs_test/{test_yaml}.yaml 2>&1 | tee -a {remotepath_logs}'
            else:
                tmp_command = f'sudo ./test.sh -t /data/test/config/configs_test/{test_yaml}.yaml -bd 2008-12-03 2>&1 | tee -a {remotepath_logs}'

            os.system(f"{'cd /data/test/bin/'} && {tmp_command}")

            shutil.copy2(remotepath_logs, test_path_logs)

    @staticmethod
    def condition_check_exception(expected_sentence, fr, search_status):
        txt = fr.read()
        try:
            if re.search(expected_sentence, txt).group() == expected_sentence:
                search_status = True

        except AttributeError as attributeError:
            logger.warn(str(attributeError))

            if re.search(expected_sentence, txt) == expected_sentence:
                search_status = True

        return search_status

    @staticmethod
    def condition_check(search_sentences, fr):

        excluded_sentences = set(search_sentences)

        logger.info(f'excluded_sentences = {search_sentences}')

        for line in fr:
            for sentence in search_sentences:
                if sentence in line:
                    excluded_sentences.remove(sentence)

        logger.info(f'excluded_sentences after remove = {excluded_sentences}')

        return excluded_sentences

    @staticmethod
    def run_command_by_host(check_hostname, configs_test_path, test_yaml, test_path_logs=None, label=None, **ssh_args):
        if check_hostname == 'cloud305':
            Helpers.run_command_cloud(configs_test_path, test_yaml, test_path_logs, label)
        elif check_hostname == 'another_host':
            Helpers.run_command_cloud_via_ssh(configs_test_path, test_yaml, test_path_logs, label, **ssh_args)

    @staticmethod
    def expected_result_partition_eom_bom(special_day):
        now = datetime.now()

        logger.info(f'now.year = {now.year}, now.month = {now.month}, now.day = {now.day}')

        expected_result = []
        for year in range(now.year, 2020, -1):

            for month in range(now.month, 0, -1):
                if month > 1:
                    last_month = month - 1
                else:
                    last_month = 12
                    year -= 1
                day = {'special': 1, 'special_2': calendar.monthrange(year=year, month=last_month)[1]}

                expected_date = date(year=year, month=last_month, day=day[special_day]).strftime('%Y-%m-%d')
                expected_result.append(expected_date)

                logger.info(f'expected_result = {expected_result}')

                if year == 2020 and month == 12:
                    break

        return expected_result[::-1]

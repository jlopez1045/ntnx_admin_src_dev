import logging
import os
import sys
import getopt

import ntnx_prism_py_client
import psutil
import urllib3
import json
import getpass
import requests
import socket
from requests.auth import HTTPBasicAuth
from base64 import b64encode
import pexpect
from pexpect import pxssh
from pathlib import Path
from packaging.version import Version
import inspect

import ntnx_lcm_py_client
from ntnx_lcm_py_client.rest import ApiException as LCMException

from ntnx_lcm_py_client.Ntnx.lcm.v4.common.PrecheckSpec import PrecheckSpec
from ntnx_lcm_py_client.Ntnx.lcm.v4.common.EntityUpdateSpec import EntityUpdateSpec
from ntnx_lcm_py_client.Ntnx.lcm.v4.common.EntityUpdateSpecs import EntityUpdateSpecs
from ntnx_lcm_py_client.Ntnx.lcm.v4.resources.RecommendationSpec import RecommendationSpec
from ntnx_lcm_py_client.Ntnx.lcm.v4.common.UpdateSpec import UpdateSpec

import configparser

import multiprocessing
from multiprocessing import Manager

from datetime import datetime
from sys import exit
from time import sleep

from tme import Utils

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dir_path)

version = '1.1.0'
filename = os.path.basename(os.path.realpath(__file__))

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

proxies = {'http': '', 'https': ''}

print("******************************************************")
print("* Nutanix " + filename)
print("* Version " + version)
print("******************************************************")

# ========================================================================================================================================================================================== CONFIG FILE

config = configparser.ConfigParser()
config_file = str(os.path.dirname(__file__)) + '/srv_config.txt'
config.read_file(open(config_file))

prism_username = config.get('USER', 'prism_username')
cli_username = config.get('USER', 'cli_username')
domain_suffix = config.get('USER', 'domain_suffix')

upgrade_attempts = config.get('UPGRADE', 'upgrade_attempts')
upgrade_method = config.get('UPGRADE', 'upgrade_method')
aos_build = config.get('UPGRADE', 'aos_build')

proxy_scheme = config.get('PROXY', 'proxy_scheme')
proxy_host = config.get('PROXY', 'proxy_host')
proxy_port = config.get('PROXY', 'proxy_port')

logfile = config.get('SETTINGS', 'logger_file')
poll_timeout = config.get('SETTINGS', 'poll_timeout')


# ============================================================================================================================================================================================== Utility


def convert_time(task_time_microseconds):
    task_time_seconds = task_time_microseconds / 1000000
    task_time = datetime.fromtimestamp(task_time_seconds)

    current_time = datetime.now()

    time_diff = current_time - task_time
    age_hours = round(time_diff.total_seconds() / 3600)

    return age_hours


def is_ip(ip):
    try:
        socket.inet_aton(ip)
        return True

    except:
        return False


def check_ping(address):
    try:
        response = os.system("ping -c 1 " + address + " > /dev/null")
        # print('=====', response)
        # and then check the response...
        if response == 0:
            pingstatus = True  # "Network Active"
        else:
            pingstatus = False  # "Network Error"

        return pingstatus

    except:
        return False


# ============================================================================================================================================================================================== Connect


def connect_ssh(srv, cmd, username, password):  # Uses PE User and Pass

    options = dict(StrictHostKeyChecking="no", UserKnownHostsFile="/dev/null")
    s = pxssh.pxssh(timeout=300, options=options)

    try:
        if not s.login(srv, username, password):
            return 'FAILED'
        else:
            s.sendline(cmd)
            s.prompt()  # match the prompt
            # print('=====', str(srv), s.before.decode('UTF-8'))  # decode to string and print everything before the prompt.
            s.logout()
            return 'DONE'

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        job_status[str(srv)] = 'FAILED: ' + str(inspect.currentframe().f_code.co_name) + str(msg)
        return 'FAILED'


def execute_api(req_type, srv, auth, api_endpoint, payload):
    api_url = 'https://' + str(srv).strip() + ':9440/' + api_endpoint
    payload = json.dumps(payload)
    headers = {
        'Authorization': auth,
        'Content-Type': 'application/json'
    }

    # print('=====', 'api_url', api_url)
    # print('=====', 'payload', payload)
    # print('=====', 'headers', headers)

    try:
        if req_type == 'post':
            response = requests.post(url=api_url, proxies=proxies, verify=False,
                                     data=payload,
                                     headers=headers
                                     )

            # print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Response POST', str(json_response))

            try:
                json_response = response.json()
                return json_response

            except Exception as msg:
                return 'FAILED'

        elif req_type == 'get':
            response = requests.get(url=api_url, proxies=proxies, verify=False,
                                    data=payload,
                                    headers=headers
                                    )

            # print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Response GET', str(json_response))

            try:
                json_response = response.json()
                return json_response

            except Exception as msg:
                return 'FAILED'

        else:
            return {'ERROR': 'API Failed'}

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'


def connect_ntnx_lcm_client(srv):
    config = ntnx_lcm_py_client.configuration.Configuration()
    config.host = str(srv)
    config.port = 9440
    config.verify_ssl = False
    config.max_retry_attempts = 1
    config.backoff_factor = 3
    config.username = str(prism_username)
    config.password = str(prism_password)

    client = ntnx_lcm_py_client.ApiClient(configuration=config)

    return client


# ================================================================================================================================================================================================ Tasks


def run_aos_upgrade(srv, build):
    try:

        api_endpoint = 'PrismGateway/services/rest/v1/genesis'

        payload = {'value': '{".oid":"ClusterManager",".method":"cluster_upgrade",".kwargs":{"nos_version":"' + str(
            build) + '","manual_upgrade":false,"ignore_preupgrade_tests":false,"skip_upgrade":false}}'}  # Upgrade
        # payload = {'value': '{".oid":"ClusterManager",".method":"cluster_upgrade",".kwargs":{"nos_version":"' + str(build) + '","manual_upgrade":false,"ignore_preupgrade_tests":false,"skip_upgrade":true}}'}  # Pre-Check Only

        json_response = execute_api(req_type='post', srv=srv, auth=prism_auth_header, api_endpoint=api_endpoint, payload=payload)

        if json_response == 'FAILED':
            return 'FAILED'

        # print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Response', str(json_response))

        return 'DONE'

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'


def run_lcm_inventory(srv):
    client = connect_ntnx_lcm_client(srv)

    inventoryApi = ntnx_lcm_py_client.InventoryApi(api_client=client)
    api_response = inventoryApi.inventory()
    task_ext_id = api_response.data["extId"]

    if api_response:
        sleep(15)

        status = check_task_loop(srv, task_ext_id, 'LCM Inventory')
        return status

    else:
        return 'FAILED'


def run_lcm_inventory_b(srv):
    utils = Utils(pc_ip=srv, username=prism_username, password=prism_password)

    client = connect_ntnx_lcm_client(srv)

    inventoryApi = ntnx_lcm_py_client.InventoryApi(api_client=client)
    api_response = inventoryApi.inventory()

    # ===== Monitor =====
    task_ext_id = api_response.data["extId"]
    task_name = 'Inventory'

    duration = utils.monitor_task(
        task_ext_id=task_ext_id,
        task_name=task_name,
        pc_ip=utils.prism_config.host,
        username=utils.prism_config.username,
        password=utils.prism_config.password,
        poll_timeout=poll_timeout
    )
    print(f"Inventory duration: {duration}.")
    # ===== Monitor =====

    note = f"LCM: Inventory duration: {duration}."
    logging.critical(str(srv) + ' ' + str(note))
    job_status[srv] = str(note)

    if api_response:
        return 'DONE'
    else:
        return 'FAILED'


def run_lcm_upgrade(srv):
    try:

        client = connect_ntnx_lcm_client(srv)

        # get LCM Recommendations
        lcm_instance = ntnx_lcm_py_client.api.RecommendationsApi(api_client=client)

        rec_spec = RecommendationSpec()
        rec_spec.entity_types = ["software"]

        recommendations = lcm_instance.get_recommendations(async_req=False, body=rec_spec)
        # print('recommendations', recommendations)

        print(f"{len(recommendations.data['entityUpdateSpecs'])} software components can be updated:")

        if not recommendations.data['entityUpdateSpecs']:
            note = "LCM: No updates available, skipping LCM Update planning."
            logging.critical(str(srv) + ' ' + str(note))
            job_status[srv] = str(note)

            return 'DONE'

        else:

            entity_update_specs = EntityUpdateSpecs()
            entity_update_specs.entity_update_specs = []

            for recommendation in recommendations.data["entityUpdateSpecs"]:
                spec = EntityUpdateSpec()
                spec.entity_uuid = recommendation["entityUuid"]
                spec.version = recommendation["version"]
                entity_update_specs.entity_update_specs.append(spec)

            if len(entity_update_specs.entity_update_specs) > 0:

                # Running Update

                install_update = True

                if install_update:

                    # create instance for LCM update
                    lcm_instance = ntnx_lcm_py_client.api.UpdateApi(api_client=client)

                    update_spec = UpdateSpec()
                    # configure the update properties, timing etc
                    update_spec.entity_update_specs = entity_update_specs.entity_update_specs

                    # skip the pinned VM prechecks
                    # WARNING: consider the implications of doing this in production
                    update_spec.skipped_precheck_flags = ["powerOffUvms"]  # That skips the pinned VM prechecks
                    update_spec.wait_in_sec_for_app_up = 60

                    # runs update
                    update = lcm_instance.update(async_req=False, body=update_spec)
                    # print('update', str(update.data))
                    update_task_ext_id = update.data["extId"]

                    sleep(15)

                    status = check_task_loop(srv, update_task_ext_id, 'LCM Update')
                    return status

                else:
                    note = "LCM: Updates Skipped."
                    logging.critical(str(srv) + ' ' + str(note))
                    job_status[srv] = str(note)

                    return 'DONE'

            else:
                note = f"LCM: No upgrade notifications available."
                logging.critical(str(srv) + ' ' + str(note))
                job_status[srv] = str(note)

                return 'DONE'

    except LCMException as lcm_exception:
        note = f"LCM: Unable to complete the requested action. Exception details: {lcm_exception}"
        logging.critical(str(srv) + ' ' + str(note))
        job_status[srv] = str(note)

        return 'FAILED'


def download_aos(srv, build):
    try:
        # build = '6.5.5.1'

        cmd = 'ncli software download name=' + str(build) + ' software-type=NOS'
        status = connect_ssh(srv, cmd, cli_username, cli_password)

        return status

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'


# ======================================================================================================================================================================================== Healing Tasks


def clean_home_drive(srv):
    try:
        cmd0 = 'find /home/log/journal/ -maxdepth 1 -mindepth 1 -type d -not -name $(cat /etc/machine-id) -exec rm ' + "'{}'" + ' +'
        cmd1 = 'find ~/data/prism -name ' + "'api_audit*'" + ' -mmin +4320 -type f -exec /usr/bin/rm ' + "'{}'" + ' +'
        cmd2 = 'find ~/data/prism/clickstream -name ' + "'client_tracking*'" + ' -mmin +4320 -type f -exec /usr/bin/rm ' + "'{}'" + ' +'
        cmd3 = 'find ~/data/prism/clickstream -name ' + "'external_client*'" + ' -mmin +4320 -type f -exec /usr/bin/rm ' + "'{}'" + ' +'

        cvm_ips = get_cmv_ips(srv)

        for x in cvm_ips:
            status = connect_ssh(str(x), cmd0, cli_username, cli_password)
            sleep(10)
            status = connect_ssh(str(x), cmd1, cli_username, cli_password)
            sleep(10)
            status = connect_ssh(str(x), cmd2, cli_username, cli_password)
            sleep(10)
            status = connect_ssh(str(x), cmd3, cli_username, cli_password)
            sleep(10)

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'


def cleanup_lcm(srv):
    try:
        cmd = 'python /home/nutanix/cluster/bin/lcm/lcm_task_cleanup.py'
        status = connect_ssh(srv, cmd, cli_username, cli_password)

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'


def clear_memory(srv):
    try:
        cmd0 = 'pkill -f /home/nutanix/bin/vip_monitor'
        cmd1 = 'genesis stop lazan anduril flow catalog cluster_config delphi && cluster start && sleep 30'

        cvm_ips = get_cmv_ips(srv)

        for x in cvm_ips:
            status = connect_ssh(str(x).strip(), cmd0, cli_username, cli_password)
            sleep(10)
            status = connect_ssh(str(x).strip(), cmd1, cli_username, cli_password)
            sleep(10)

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'


def reset_genesis(srv):
    try:
        cmd = 'genesis restart && genesis stop prism && cluster start'
        status = connect_ssh(srv, cmd, cli_username, cli_password)

        sleep(300)

        cvm_ips = get_cmv_ips(srv)  # Get all CVM IPs

        for x in cvm_ips:
            status = connect_ssh(str(x).strip(), cmd, cli_username, cli_password)
            sleep(300)

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'


def rolling_reboot_cvm(srv):
    try:
        cmd = 'echo Y | rolling_restart'

        cvm_ips = get_cmv_ips(srv)  # Get all CVM IPs
        num_cvms = len(cvm_ips)

        sleep_time = num_cvms * 300

        status = connect_ssh(srv, cmd, cli_username, cli_password)

        sleep(sleep_time)

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'


# =============================================================================================================================================================================================== Checks


def check_all_versions(srv):
    print('AOS')
    print('LCM')


def check_download_status(srv, build):
    try:

        id = '1708078628346'
        api_endpoint = 'PrismGateway/services/rest/v1/upgrade/nos/softwares?_=' + str(id)

        payload = {}

        json_response = execute_api(req_type='get', srv=srv, auth=prism_auth_header, api_endpoint=api_endpoint, payload=payload)

        if json_response == 'FAILED':
            return 'FAILED'

        # print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Response', str(json_response))

        entities_list = json_response['entities']

        for x in entities_list:
            if x['name'] == build:
                return str(x['status'])

        return 'FAILED'

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        job_status[str(srv)] = 'FAILED: ' + str(inspect.currentframe().f_code.co_name) + str(msg)
        return 'FAILED'


def check_lcm_task(srv):
    try:
        client = connect_ntnx_lcm_client(srv)

        statusApi = ntnx_lcm_py_client.StatusApi(api_client=client)
        api_response = vars(statusApi.get_status())

        if api_response:

            data = api_response['_GetLcmStatusApiResponse__data']
            # print('check_lcm_task', data['inProgressOperation'])

            var_uuid = str(data['inProgressOperation'].get('uuid')).upper()
            var_task = str(data['inProgressOperation'].get('type')).upper()

            if var_uuid:
                print('check_lcm_task', str(var_uuid), str(var_task).upper())
                return str(var_uuid), str(var_task).upper()

            else:
                return 'NONE'

        else:
            return 'FAILED FUNCTION'

    except:
        return 'FAILED FUNCTION'


def check_task_uuid(srv, uuid):
    try:

        api_endpoint = 'PrismGateway/services/rest/v2.0/tasks/' + str(uuid)

        payload = {}

        json_response = execute_api(req_type='get', srv=srv, auth=prism_auth_header, api_endpoint=api_endpoint, payload=payload)

        # print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Response', str(json_response))

        progress_status = str(json_response.get('progress_status')).upper()
        percentage_complete = str(json_response.get('percentage_complete')).upper()

        if progress_status == 'FAILED':
            return 'FAILED'
        elif progress_status == 'RUNNING':
            return str('RUNNING ' + str(percentage_complete))
        elif progress_status == 'SUCCEEDED':
            return 'DONE'
        else:
            return progress_status

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'


def check_task_loop(srv, uuid, task_name):

    while True:
        status = check_task_uuid(srv, uuid)

        if status == 'FAILED':
            note = "FAILED: " + str(task_name)
            logging.critical(str(srv) + ' ' + str(note))
            job_status[srv] = str(note)

            return 'FAILED'

        elif status == 'DONE':
            note = "Completed: " + str(task_name)
            logging.critical(str(srv) + ' ' + str(note))
            job_status[srv] = str(note)

            return 'DONE'

        elif status == 'RUNNING':
            percent_complete = str(status).split(' ')
            note = "RUNNING: " + str(task_name) + " - " + str(percent_complete[1]) + str('%')
            logging.critical(str(srv) + ' ' + str(note))
            job_status[srv] = str(note)

        else:
            note = "RUNNING: " + str(task_name) + " " + str(status)
            logging.critical(str(srv) + ' ' + str(note))
            job_status[srv] = str(note)

        sleep(60)


def check_lcm_upgrade_task(srv):
    try:

        api_endpoint = 'PrismGateway/services/rest/v2.0/tasks/list'

        payload = {}

        json_response = execute_api(req_type='post', srv=srv, auth=prism_auth_header, api_endpoint=api_endpoint, payload=payload)

        # print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Response', str(json_response))

        if json_response == 'FAILED':
            return 'FAILED'

        entities_list = json_response['entities']
        # entities_list.sort(lambda x: x['start_time_usecs'])

        if len(entities_list) == 0:
            return 'MISSING'

        task_names = ['klcmroottask']

        found_matching_task = False
        pass_upgrade = False

        for task in task_names:

            for x in reversed(entities_list):

                task_created_time = x['create_time_usecs']
                age_in_hours = convert_time(task_created_time)

                if age_in_hours < 24:

                    if str(x['operation_type']).lower() == task:

                        found_matching_task = True

                        percentage = x['percentage_complete']
                        progress = x['progress_status']

                        if str(progress).upper() == 'SUCCEEDED':

                            return 'DONE'

                        elif str(progress).upper() == 'RUNNING':

                            return 'RUNNING: LCM Task Percentage: ' + str(percentage) + ' %'

                        elif str(progress).upper() == 'FAILED':
                            task = "FAILED: LCM Task"

                            pass_upgrade = False

        if not pass_upgrade:

            if found_matching_task:
                task = "FAILED: LCM Task"
            else:
                task = 'MISSING'
            return task

        else:
            return 'MISSING'

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'


def check_upgrade_task(srv):
    try:

        api_endpoint = 'PrismGateway/services/rest/v2.0/tasks/list'

        payload = {}

        json_response = execute_api(req_type='post', srv=srv, auth=prism_auth_header, api_endpoint=api_endpoint, payload=payload)

        # print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Response', str(json_response))

        if json_response == 'FAILED':
            return 'FAILED'

        entities_list = json_response['entities']
        # entities_list.sort(lambda x: x['start_time_usecs'])

        if len(entities_list) == 0:
            return 'MISSING'

        task_names = ['kclusterpreupgradetask', 'kclusterupgradetask']

        found_matching_task = False
        pass_pre_upgrade = False
        pass_upgrade = False

        for task in task_names:

            for x in reversed(entities_list):

                task_created_time = x['create_time_usecs']
                age_in_hours = convert_time(task_created_time)

                if age_in_hours < 24:

                    if str(x['operation_type']).lower() == task:

                        found_matching_task = True

                        percentage = x['percentage_complete']
                        progress = x['progress_status']

                        if str(progress).upper() == 'SUCCEEDED':

                            if task == 'kclusterupgradetask':
                                pass_upgrade = True
                                return 'DONE'

                            if task == 'kclusterpreupgradetask':
                                pass_pre_upgrade = True
                                break

                        elif str(progress).upper() == 'RUNNING':

                            if task == 'kclusterupgradetask':
                                return 'RUNNING: Upgrade Task Percentage: ' + str(percentage) + ' %'

                            elif task == 'kclusterpreupgradetask':
                                return 'RUNNING: Pre Upgrade Task Percentage: ' + str(percentage) + ' %'

                        elif str(progress).upper() == 'FAILED':
                            task = "FAILED: Upgrade Task"

                            if task == 'kclusterupgradetask':
                                pass_upgrade = False

                            elif task == 'kclusterpreupgradetask':
                                pass_pre_upgrade = False

        if not pass_pre_upgrade:

            if found_matching_task:
                task = "FAILED: Pre Upgrade Task"
            else:
                task = 'MISSING'
            return task

        elif not pass_upgrade:

            if found_matching_task:
                task = "FAILED: Upgrade Task"
            else:
                task = 'MISSING'
            return task

        else:
            return 'ERROR'

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'


# ============================================================================================================================================================================================= Get Info

def get_cluster_info(srv):
    try:
        api_endpoint = 'api/nutanix/v2.0/cluster/'

        payload = {}

        json_response = execute_api(req_type='get', srv=srv, auth=prism_auth_header, api_endpoint=api_endpoint, payload=payload)

        if json_response == 'FAILED':
            return 'FAILED'

        # print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Response', str(json_response))

        try:
            uuid = json_response['cluster_uuid']
            return uuid

        except Exception as msg:
            return 'FAILED'

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'


def get_cmv_ips(srv):
    try:
        api_endpoint = 'PrismGateway/services/rest/v2.0/hosts/'

        payload = {}

        json_response = execute_api(req_type='get', srv=srv, auth=prism_auth_header, api_endpoint=api_endpoint, payload=payload)

        if json_response == 'FAILED':
            return 'FAILED'

        # print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Response', str(json_response))

        entities = json_response['entities']
        cvm_ips = []

        for x in entities:
            ip = x['controller_vm_backplane_ip']
            cvm_ips.append(str(ip).strip())

        return cvm_ips

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'


def get_cluster_build(srv):
    try:
        id = '1708238042442'
        api_endpoint = 'PrismGateway/services/rest/v1/cluster/version?__=' + str(id)

        payload = {}

        json_response = execute_api(req_type='get', srv=srv, auth=prism_auth_header, api_endpoint=api_endpoint, payload=payload)

        if json_response == 'FAILED':
            return 'FAILED'

        # print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Response', str(json_response))

        ver = str(json_response['version'])
        build = ver.split('-')
        build_number = str(build[1])

        return build_number

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)
        return 'FAILED'


# ========================================================================================================================================================================================= Active Loops


def record_status(job_status, logging):
    sleep(30)

    while True:

        print('****************************************************** STATUS ***** START')

        for key, value in job_status.items():
            print('Server: ' + str(key), 'Status: ' + str(value))
            logging.critical('Server: ' + str(key) + ' Status: ' + str(value))

        print('****************************************************** STATUS ******* END')

        sleep(120)  # 2 min


def upgrade_loop(srv, build, job_status, logging):
    try:

        task_rolling_reboot = False
        task_genesis_restart = False
        task_memory_clean = False
        task_drive_clean = False
        task_lcm_cleanup = False

        task_count_lcm_updates = 0

        retry_count_download = 0
        retry_count_upgrade = 0

        # ===== Ping Check ===== Start
        pingable = check_ping(srv)
        if not pingable:
            job_status[srv] = 'FAILED: Ping Test - Quitting'
            return
        # ===== Ping Check ===== End

        # ===== Password Check ===== Start
        status = connect_ssh(srv, "echo 'I am Connected'", cli_username, cli_password)
        if status == 'FAILED':
            note = 'FAILED: Password Check - CLI Account - ' + str(cli_username) + ' - Quitting'
            logging.critical(str(srv) + ' ' + str(note))
            job_status[srv] = str(note)
            return

        sleep(2)

        status = get_cluster_info(srv)
        if status == 'FAILED':
            note = 'FAILED: Password Check - Prism Account - ' + str(prism_username) + ' - Quitting'
            logging.critical(str(srv) + ' ' + str(note))
            job_status[srv] = str(note)
            return
        # ===== Password Check ===== End

        sleep(5)

        # ===== AOS Cluster Version Check ===== Start

        if upgrade_method == 'aos_only':

            cluster_ver = get_cluster_build(srv)

            if cluster_ver == 'FAILED':
                note = 'FAILED: Password Check - Cli Account - ' + str(cli_username) + ' - Quitting'
                logging.critical(str(srv) + ' ' + str(note))
                job_status[srv] = str(note)
                return

            elif Version(cluster_ver) >= Version(build):  # No upgrade needed
                note = 'Current: ' + str(cluster_ver) + ' - Quitting'
                logging.critical(str(srv) + ' ' + str(note))
                job_status[str(srv)] = str(note)
                return

            else:
                note = 'Current: ' + str(cluster_ver) + ' - Upgrade Needed'
                logging.critical(str(srv) + ' ' + str(note))

        # ===== AOS Cluster Version Check ===== End

        # ===== Set Upgrade Path ===== Start

        if upgrade_method == 'aos_only':

            job_lcm_inventory = False
            job_lcm_upgrade = False
            job_download = True
            job_upgrade = True

        elif upgrade_method == 'lcm_only':

            job_lcm_inventory = True
            job_lcm_upgrade = True
            job_download = False
            job_upgrade = False

        else:

            note = 'ERROR: ' + str(cluster_ver) + ' Upgrade Method missing - Quitting'
            logging.critical(str(srv) + ' ' + str(note))
            return

        # ===== Set Upgrade Path ===== End

        while True:

            if upgrade_method == 'lcm_only':

                if job_lcm_inventory or job_lcm_upgrade:

                    if job_lcm_inventory:

                        status = check_lcm_task(srv)
                        uuid = status[0]

                        if status == 'FAILED FUNCTION':
                            note = 'FAILED: LCM Task Check - Quitting'
                            logging.critical(str(srv) + ' ' + str(note))
                            job_status[srv] = str(note)
                            return

                        elif status == 'NONE':

                            status = run_lcm_inventory(srv)

                            if status == 'FAILED':
                                note = 'FAILED: LCM Inventory - Quitting'
                                logging.critical(str(srv) + ' ' + str(note))
                                job_status[srv] = str(note)
                                return

                        else:

                            status = check_task_loop(srv, uuid, 'LCM Inventory')

                            if status == 'FAILED':
                                note = 'FAILED: LCM Inventory - Quitting'
                                logging.critical(str(srv) + ' ' + str(note))
                                job_status[srv] = str(note)
                                return

                    if job_lcm_upgrade:

                        while task_count_lcm_updates < 3:

                            status = check_lcm_task(srv)
                            uuid = status[0]

                            if status == 'FAILED FUNCTION':
                                note = 'FAILED: LCM Task Check - Quitting'
                                logging.critical(str(srv) + ' ' + str(note))
                                job_status[srv] = str(note)
                                return

                            elif status == 'NONE':

                                status = run_lcm_upgrade(srv)

                                if status == 'FAILED':
                                    note = 'FAILED: LCM Update - Quitting'
                                    logging.critical(str(srv) + ' ' + str(note))
                                    job_status[srv] = str(note)
                                    return

                            else:

                                task_count_lcm_updates = task_count_lcm_updates + 1

                                status = check_task_loop(srv, uuid, 'LCM Update')

                                if status == 'FAILED':
                                    note = 'FAILED: LCM Update - Quitting'
                                    logging.critical(str(srv) + ' ' + str(note))
                                    job_status[srv] = str(note)
                                    return

                                sleep(60)

                    sleep(10)
                    print('=============================== ENDING', str(srv))
                    return

            elif upgrade_method == 'aos_only':

                if job_download:

                    note = 'ACTION: check_download_status'
                    logging.info(str(srv) + ' ' + str(note))
                    job_status[str(srv)] = str(note)

                    status = check_download_status(srv, build)

                    note = 'DOWNLOAD: ' + str(status)
                    logging.info(str(srv) + ' ' + str(note))
                    job_status[str(srv)] = str(note)

                    retry_count_download = retry_count_download + 1

                    if (status == 'AVAILABLE') or (status == 'FAILED'):
                        if not task_drive_clean:
                            note = 'ACTION: clean_home_drive A - Attempt ' + str(retry_count_download)
                            logging.info(str(srv) + ' ' + str(note))
                            job_status[str(srv)] = str(note)

                            clean_home_drive(srv)
                            task_drive_clean = True

                            sleep(120)
                            continue

                        elif not task_memory_clean:
                            note = 'ACTION: clear_memory A - Attempt ' + str(retry_count_download)
                            logging.info(str(srv) + ' ' + str(note))
                            job_status[str(srv)] = str(note)

                            clear_memory(srv)
                            task_memory_clean = True

                            sleep(120)
                            continue

                        elif not task_lcm_cleanup:
                            cleanup_lcm(srv)

                            note = 'ACTION: cleanup_lcm B - Attempt ' + str(retry_count_download)
                            logging.info(str(srv) + ' ' + str(note))
                            job_status[str(srv)] = str(note)

                            cleanup_lcm(srv)
                            task_lcm_cleanup = True

                            sleep(120)
                            continue

                        if retry_count_download > 4:
                            note = 'FAILED: check_download_status A - Quitting'
                            logging.critical(str(srv) + ' ' + str(note))
                            job_status[srv] = str(note)
                            return

                        else:
                            download_aos(srv, build)
                            sleep(120)
                            continue

                    elif status == 'INPROGRESS':
                        sleep(120)
                        continue

                    elif status == 'COMPLETED':
                        note = 'DONE: download_aos ' + str(status)
                        logging.critical(str(srv) + ' ' + str(note))
                        job_status[srv] = str(note)

                        job_download = False
                        job_upgrade = True

                        continue

                elif job_upgrade:

                    note = 'ACTION: check_upgrade_task'
                    logging.info(str(srv) + ' ' + str(note))
                    job_status[str(srv)] = str(note)

                    status = check_upgrade_task(srv)

                    note = 'UPGRADE: ' + str(status)
                    logging.info(str(srv) + ' ' + str(note))
                    job_status[str(srv)] = str(note)

                    if 'DONE' in status:
                        cluster_ver = get_cluster_build(srv)

                        if cluster_ver == 'FAILED':
                            note = 'FAILED: get_cluster_build - Quitting'
                            logging.critical(str(srv) + ' ' + str(note))
                            job_status[srv] = str(note)
                            return

                        elif Version(cluster_ver) >= Version(build):  # No upgrade needed
                            note = 'Current: ' + str(cluster_ver) + ' Quitting'
                            logging.critical(str(srv) + ' ' + str(note))
                            job_status[str(srv)] = str(note)
                            return

                        else:
                            note = 'Current: ' + str(cluster_ver)
                            logging.critical(str(srv) + ' ' + str(note))
                            job_status[str(srv)] = str(note)

                            sleep(120)
                            continue

                    elif 'RUNNING' in status:
                        note = 'UPGRADE: ' + str(status)
                        logging.critical(str(srv) + ' ' + str(note))
                        job_status[str(srv)] = str(note)

                        sleep(120)
                        continue

                    elif 'MISSING' in status:
                        note = 'ACTION: run_aos_upgrade A'
                        logging.critical(str(srv) + ' ' + str(note))
                        job_status[str(srv)] = str(note)

                        run_aos_upgrade(srv, build)

                        sleep(120)
                        continue

                    elif 'FAILED' in status:

                        retry_count_upgrade = retry_count_upgrade + 1

                        if retry_count_upgrade > 3:
                            job_status[srv] = 'FAILED: Upgrade Task - Quitting'
                            return

                        if not task_rolling_reboot:
                            note = 'ACTION: rolling_reboot_cvm A / Attempt: ' + str(retry_count_upgrade)
                            logging.info(str(srv) + ' ' + str(note))
                            job_status[str(srv)] = str(note)

                            rolling_reboot_cvm(srv)
                            task_rolling_reboot = True

                            sleep(120)
                            continue

                        '''
                        elif not task_genesis_restart:
                            note = 'ACTION: reset_genesis A / Attempt: ' + str(retry_count_upgrade)
                            logging.info(str(srv) + ' ' + str(note))
                            job_status[str(srv)] = str(note)
    
                            reset_genesis(srv)
                            task_genesis_restart = True
    
                            sleep(120)
                            continue
                        '''

                        note = 'ACTION: run_aos_upgrade B / Attempt: ' + str(retry_count_upgrade)
                        logging.critical(str(srv) + ' ' + str(note))
                        job_status[str(srv)] = str(note)

                        run_aos_upgrade(srv, build)

                        sleep(120)
                        continue

                    elif status == 'ERROR':
                        note = 'ERROR: check_upgrade_task - Retrying '
                        logging.critical(str(srv) + ' ' + str(note))
                        job_status[srv] = str(note)
                        continue

    except Exception as msg:
        print('=====', str(srv), inspect.currentframe().f_code.co_name, 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(msg).__name__, msg)


if __name__ == "__main__":

    # ======================================================================================================================================================================================== Load List
    try:

        try:
            list_pc = open("PC_List.txt", "r")
        except Exception as error:
            print('ERROR', error)

        try:
            # print(sys.argv)
            opts, args = getopt.getopt(sys.argv[1:], "h:c:", ["cfile="])

            for opt, arg in opts:
                # print(opt)

                if opt == '-h':
                    print('test.py -c <clusterlist.txt>')
                    sys.exit()

                elif opt in ("-c", "--cfile"):
                    list_cluster = open(str(arg), "r")

        except getopt.GetoptError:
            print('test.py -c <clusterlist.txt>')
            sys.exit(2)

        try:

            prism_password = getpass.getpass(
                prompt='Please enter your Prism Element password: ',
                stream=None,
            )

            cli_password = getpass.getpass(
                prompt='Please enter your CLI password: ',
                stream=None,
            )

            prism_encoded_credentials = b64encode(bytes(f"{prism_username}:{prism_password}", encoding="ascii")).decode("ascii")
            prism_auth_header = f"Basic {prism_encoded_credentials}"

            cli_encoded_credentials = b64encode(bytes(f"{cli_username}:{cli_password}", encoding="ascii")).decode("ascii")
            cli_auth_header = f"Basic {cli_encoded_credentials}"

            # print(encoded_credentials, auth_header)

        except Exception as error:
            print('ERROR', error)
            sys.exit()

        # =========================================================================================================================================================================== Load Log File Settings

        logging.basicConfig(filename=logfile,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.INFO)

        # ======================================================================================================================================================================================= Start Loop

        print('****************************************************** Start')

        logging.info("Script Starting")

        jobs = []
        job_pid = []

        manager = Manager()
        job_status = manager.dict()

        process = multiprocessing.Process(target=record_status, args=(job_status, logging))
        jobs.append(process)

        for x in list_cluster:

            x = str(x).strip()

            if not is_ip(x):
                if domain_suffix not in x:
                    x = str(x) + '.' + domain_suffix

            job_status[str(x)] = 'Starting'

            if len(x) > 1:
                process = multiprocessing.Process(target=upgrade_loop, args=(x, aos_build, job_status, logging))
                jobs.append(process)

        for j in jobs:
            j.start()

        for j in jobs:
            j.join()

        print('****************************************************** End')

    except KeyboardInterrupt:
        print('Attempting to close all threads')

        for j in jobs:
            j.kill()

        print('Closed')

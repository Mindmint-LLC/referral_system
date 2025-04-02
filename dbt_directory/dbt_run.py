
import os
from prefect import flow, task
from dbt.cli.main import dbtRunner
from prefect.blocks.notifications import SlackWebhook
from prefect.logging import get_run_logger
from prefect.blocks.system import Secret
import json


def get_res_message(res):
    message = ''
    for r in res.result:
        if r.status in ['error', 'fail', 'warn']:
            message += f"{r.node.name}: {r.message}\n"
    if message == '':
        return None
    else:
        return message


@task
def send_slack_notification(res):
    if res.success:
        message = get_res_message(res)
        if message:
            message = f"mm_referral_program:\n{message}"
            slack_webhook_block = SlackWebhook.load("slack-notifications")
            slack_webhook_block.notify(message)


@task
def error_handling(res):
    message = None
    if len(res.result) == 0:
        message = 'No results from dbt run'
    if not res.success:
        message = get_res_message(res)
    if message:
        raise Exception(message)


@task
def log_results(res):
    num_success = 0
    num_warn = 0
    num_error = 0
    num_skip = 0
    num_total = 0
    bytes_processed = 0
    error_list = ''
    warn_list = ''

    for x in res.result:
        if x.status == 'success' or x.status == 'pass':
            num_success += 1
        elif x.status == 'warn':
            num_warn += 1
            warn_list += x.node.name + ' - ' + x.message + '\n'
        elif x.status == 'error' or x.status == 'fail':
            num_error += 1
            error_list += x.node.name + ' - ' + x.message + '\n'
        elif x.status == 'skipped':
            num_skip += 1
        try:
            bytes_processed += x.adapter_response['bytes_billed']
        except:
            pass
        num_total += 1

    elapsed_time = res.result.elapsed_time

    results_dict = {
        'is_success': 1 if num_success == num_total else 0,
        'num_success': num_success,
        'num_warn': num_warn,
        'num_error': num_error,
        'num_skip': num_skip,
        'num_total': num_total,
        'elapsed_time': elapsed_time,
        'gb_processed': round(bytes_processed / (1000 ** 3), 1),
        'errors': error_list,
        'warnings': warn_list,
    }

    message = []
    for key, value in results_dict.items():
        message.append(f"{key}: {value}")
    logger = get_run_logger()
    logger.info('\n'.join(message))


@task
def trigger_dbt_flow() -> str:
    bigquery_block = Secret.load("bbg-bigquery-sa")
    bigquery_value = bigquery_block.get()
    with open('bigquery-bbg-platform.json', 'w') as f:
        json.dump(bigquery_value, f)
    os.environ['DBT_KEYFILE'] = 'bigquery-bbg-platform.json'

    dbt = dbtRunner()
    dbt.invoke(["deps"])
    res = dbt.invoke(["build"])
    # res = dbt.invoke(["run", "--select", "dim_products"])
    return res


@flow
def run_dbt_flow():
    directory = os.path.dirname(os.path.abspath(__file__))
    os.chdir(directory)
    res = trigger_dbt_flow()
    t_log = log_results(res)
    t_slack = send_slack_notification(res)
    error_handling.submit(res, wait_for=[t_log, t_slack]).result()
        

if __name__ == "__main__":
    run_dbt_flow.serve()
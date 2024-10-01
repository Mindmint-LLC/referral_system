#%% Template Imports

import os
import dlogging
from demail.gmail import SendEmail
# import importlib
import json


package_name = ''
logger = dlogging.NewLogger(__file__, use_cd=True)
logger.info('Beginning package')


try:

    filepath = '/git_repo/app/dbt/target/run_results.json'
    with open(filepath, 'r') as f:
        js = f.read()
    js = json.loads(js)

    error_list = ''
    for x in js['results']:
        if x['status'] == 'error':
            error_list += x['unique_id'] + ' - ' + x['message'] + '\n\n'
    if error_list != '':
        raise Exception(error_list)

    import post_credits

    logger.info('Done! No problems.\n')


except Exception as e:
    e = str(e)
    logger.critical(f'{e}\n', exc_info=True)
    SendEmail(to_email_addresses=os.getenv('EMAIL_FAIL')
                        , subject=f'Python Error - {package_name}'
                        , body=e
                        , attach_file_address=logger.handlers[0].baseFilename
                        , user=os.getenv('EMAIL_UID')
                        , password=os.getenv('EMAIL_PWD')
                        )
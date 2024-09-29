#%%

import os
import requests
from dbharbor.bigquery import SQL
import dbharbor
import dlogging

logger = dlogging.NewLogger(__file__, use_cd=True)


#%%

logger.info('Gather env vars')
con = SQL(os.getenv('DBT_KEYFILE'))
username = os.getenv('MM_API_UID')
password = os.getenv('MM_API_PWD')
url = os.getenv('MM_API_URL')
schema = os.getenv("DBT_DATASET")


#%%

# import pandas as pd
# df = pd.DataFrame({'affiliate_code':['abc', 'def'], 'credits':[100, 300], 'points':[1000, 2000], 'cumulative_points':[5000, 8000]})

logger.info('Pull unapplied credits from Bigquery')
sql = f'''
    select pk
    , referrer_id as affiliate_code
    , credits
    , points
    , points as cumulative_points
    from `bbg-platform.{schema}.fct_credit`
'''
df = con.read(sql)


if not df.empty:
    payload = {}
    df_payload = df[['affiliate_code', 'credits', 'points', 'cumulative_points']].copy()
    payload['payload'] = df_payload.to_json(orient='records')
    logger.info(f'Post credits:\n{payload}')

    response = requests.post(url, auth=(username, password), json=payload)
    if response.status_code == 200:
        logger.info('Credits successfully applied, updating applied table in Bigquery')
        df_applied = df[['pk']].copy()
        df_applied = dbharbor.clean(df_applied, rowloadtime=True)
        con.to_sql(df_applied, f'`bbg-platform.{schema}.fct_credit_applied`', index=False, if_exists='append')
        logger.info('Applied table successfully updated')
    else:
        logger.error('Error during credit post')
        raise(f'{response.status_code}\n{response.text}')
else:
    logger.info('No credits to apply')


#%%
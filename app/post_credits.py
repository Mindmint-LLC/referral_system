#%%

import os
import requests
import pandas as pd
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

logger.info('Pull unapplied credits from Bigquery')
sql = f'''
    select c.pk
    , c.referrer_id as affiliate_code
    , c.credits
    , c.points
    , c.points_agg_new as cumulative_points
    from `bbg-platform.{schema}.fct_mm_api_summary` c
    left join `bbg-platform.{schema}.fct_mm_api_summary_applied` a
        on c.pk = a.pk
    where a.pk is null;
'''
df = con.read(sql)


#%%

if not df.empty:
    payload = {}
    df_payload = df.groupby('affiliate_code').agg({'credits':'sum', 'points':'sum', 'cumulative_points':'max'}).copy()
    df_payload.reset_index(inplace=True)
    payload['payload'] = df_payload.to_dict(orient='records')
    logger.info(f'Post credits:\n{payload}')

    response = requests.post(url, auth=(username, password), json=payload)
    if response.status_code == 200:
        logger.info('Credits successfully applied, updating applied table in Bigquery')
        df_applied = df[['pk']].copy()
        df_applied = dbharbor.clean(df_applied, rowloadtime=True)
        con.to_sql(df_applied, f'{schema}.fct_mm_api_summary_applied', index=False, if_exists='append')
        logger.info('Applied table successfully updated')
    else:
        logger.error('Error during credit post')
        raise Exception(f'{response.status_code}\n{response.text}')
else:
    logger.info('No credits to apply')


#%%
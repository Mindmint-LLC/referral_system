{{
  config(
    materialized = 'incremental',
    unique_key = 'referrer_id',
    )
}}

with base as (
    select distinct t.referrer_id
    from {{ ref('int_trial') }} t
    {% if is_incremental() %}
        where not exists (
            select null
            from {{ this }} l
            where t.referrer_id = l.referrer_id
        )
    {% endif %}
)

select b.referrer_id
    , CURRENT_TIMESTAMP() as row_load_date_time
from base b
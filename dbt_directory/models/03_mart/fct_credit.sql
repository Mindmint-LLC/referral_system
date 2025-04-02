
{{
  config(
    materialized = 'incremental',
    unique_key = 'pk',
    )
}}

with base as (
    select r.referrer_id
        , sum(t.points) as points
    from {{ ref('fct_referrer') }} r
        join {{ ref('fct_referral') }} t
            on r.referrer_id = t.referrer_id
    group by all
)

, credits as (
    select {{ dbt_utils.generate_surrogate_key(['b.referrer_id', 'c.level']) }} as pk
        , b.referrer_id
        , c.level
        , c.points
        , c.credits
        , CURRENT_TIMESTAMP() as row_load_date_time
    from base b
        join {{ ref('dim_credit') }} c
            on b.points >= c.points
)

select c.*
from credits c
{% if is_incremental() %}
where not exists (
    select null
    from {{ this }} t
    where c.pk = t.pk
)
{% endif %}
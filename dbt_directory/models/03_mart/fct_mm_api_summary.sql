
{% if execute %}
  {% if flags.FULL_REFRESH %}
      {{ exceptions.raise_compiler_error("Full refresh is not allowed for this model. Exclude it from the run via the argument \"--exclude model_name\".") }}
  {% endif %}
{% endif %}

{{
  config(
    materialized = 'incremental',
    unique_key = 'pk',
    )
}}

with credits as (
  select c.referrer_id
    , sum(c.credits) as credits_agg_new
  from {{ ref('fct_credit') }} c
  group by all
)

, points as (
  select r.referrer_id
    , sum(r.points) as points_agg_new
  from {{ ref('fct_referral') }} r
  group by all
)

{% if is_incremental() %}

  , last_run as (
    select s.referrer_id
      , s.credits_agg_new as credits_agg_old
      , s.points_agg_new as points_agg_old
    from {{ this }} s
    qualify row_number() over (partition by s.referrer_id order by s.row_load_date_time desc) = 1
  )

  select {{ dbt_utils.generate_surrogate_key(['r.referrer_id', 'CURRENT_TIMESTAMP()']) }} as pk
    , r.referrer_id
    , c.credits_agg_new
    , l.credits_agg_old
    , c.credits_agg_new - coalesce(l.credits_agg_old, 0) as credits
    , p.points_agg_new
    , l.points_agg_old
    , p.points_agg_new - coalesce(l.points_agg_old, 0) as points
    , CURRENT_TIMESTAMP() as row_load_date_time
  from {{ ref('fct_referrer') }} r
    left join last_run l
      on r.referrer_id = l.referrer_id
    left join credits c
      on r.referrer_id = c.referrer_id
    left join points p
      on r.referrer_id = p.referrer_id
  where c.credits_agg_new - coalesce(l.credits_agg_old, 0) > 0
    or p.points_agg_new - coalesce(l.points_agg_old, 0) > 0

{% else %}

  select {{ dbt_utils.generate_surrogate_key(['r.referrer_id', 'CURRENT_TIMESTAMP()']) }} as pk
    , r.referrer_id
    , c.credits_agg_new
    , 0 as credits_agg_old
    , c.credits_agg_new as credits
    , p.points_agg_new
    , 0 as points_agg_old
    , p.points_agg_new as points
    , CURRENT_TIMESTAMP() as row_load_date_time
  from {{ ref('fct_referrer') }} r
    left join credits c
      on r.referrer_id = c.referrer_id
    left join points p
      on r.referrer_id = p.referrer_id
  where c.credits_agg_new > 0
    or p.points_agg_new > 0

{% endif %}
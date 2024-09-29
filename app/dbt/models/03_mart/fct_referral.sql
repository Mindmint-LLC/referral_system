
{{
  config(
    materialized = 'incremental',
    unique_key = 'id_tracking_order',
    )
}}

select t.*
    , s.points
    , CURRENT_TIMESTAMP() as row_load_date_time
from {{ ref('int_trial__status') }} t
    join {{ ref('dim_points') }} s
        on t.sub_category = s.sub_category
where t.is_good_referral = 1

{% if is_incremental() %}
    and not exists (
        select null
        from {{ this }} l
        where t.id_tracking_order = l.id_tracking_order
    )
{% endif %}
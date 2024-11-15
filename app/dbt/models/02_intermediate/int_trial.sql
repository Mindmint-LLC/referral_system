
with base as (
    SELECT t.id as id_tracking_order
        , analytics.fnEmail(t.email) as email
        , t.email as email_orig
        , timestamp(date_add(t.dt, interval 7 hour)) as dt
        , {{ dbt_utils.generate_surrogate_key(['analytics.fnEmail(t.email)', 'cast(timestamp(date_add(t.dt, interval 7 hour)) as date)']) }} as uq_email_created
        , replace(JSON_EXTRACT(t.json, '$.purchase.subscription_id'), '"', '') as subscription_id
        , split(replace(JSON_EXTRACT(t.json, '$.purchase.contact.cart_affiliate_id'), '"', ''), "\\")[3] as referrer_id
        {# , SUBSTRING_INDEX(SUBSTRING_INDEX(JSON_EXTRACT(json, '$.purchase.contact.cart_affiliate_id'), "\\", 4), '"', -1) as referral_id #}
        , t.json
    FROM {{ source('kbb_evergreen', 'tracking_orders') }} t
    where funnel_id = cast({{ env_var('FUNNEL_ID') }} as string)
        and JSON_EXTRACT(json, '$.purchase.contact.cart_affiliate_id') like '%affiliate_id%'
)

select *
from base b
where nullif(b.referrer_id, '') is not null
qualify row_number() over (partition by b.email order by b.dt asc) = 1
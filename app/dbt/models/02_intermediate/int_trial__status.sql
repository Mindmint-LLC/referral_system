
with subscription_item as (
    select *
    from {{ source('stripe_mastermind', 'subscription_item') }} si
    qualify row_number() over (partition by si.subscription_id order by si.created desc) = 1
)

{% if target.schema == 'referral_system' %}
    , dim_product as (
        select p.product
            , p.sub_category
        from analytics.dim_products p
        where p.mastermind_subscription_type = 'subscription'
    )
{% else %}
    , dim_product as (
        select p.product
            , p.sub_category
        from {{ this.schema }}.dim_products p
    )
{% endif %}

, all_subscriptions as (
    select sh.id as subscription_id_mm
        , sh.created
        , coalesce(sh.cancel_at, sh.canceled_at) as cancel_date
        , coalesce(sh.cancel_at, sh.canceled_at, cast('9000-12-31' as timestamp)) as ref_date
        , sh.customer_id
        , analytics.fnEmail(cs.email) as email
        , {{ dbt_utils.generate_surrogate_key(['analytics.fnEmail(cs.email)', 'cast(sh.created as date)']) }} as uq_email_created
        , si.plan_id as product
        , p.sub_category
        , case when p.product is not null and lower(sh.metadata) like '%referral%' then true else false end as is_good_subscription
    from {{ source('stripe_mastermind', 'subscription_history') }} sh
        join subscription_item si
            on sh.id = si.subscription_id
        LEFT JOIN {{ source('stripe_mastermind', 'customer') }} cs
            ON sh.customer_id = cs.id
        left join dim_product p
            on si.plan_id = p.product
)

, new_subscription as (
    select *
    from {{ ref('int_trial') }} t
        join all_subscriptions s
            on t.uq_email_created = s.uq_email_created
            and s.is_good_subscription = true
    qualify row_number() over (partition by t.id_tracking_order order by s.ref_date desc, s.created desc) = 1
)

, old_subscription as (
    select distinct t.id_tracking_order
    from {{ ref('int_trial') }} t
        join all_subscriptions s
            on t.email = s.email
            and cast(t.dt as date) > date_add(cast(s.ref_date as date), interval -6 month)
            and cast(t.dt as date) > cast(s.created as date)
)

, good_payment as (
    select sh.id_tracking_order
        , min(i.due_date) as first_paid
    from new_subscription sh
        join {{ source('stripe_mastermind', 'invoice') }} i
            on sh.subscription_id_mm = i.subscription_id
            and i.status = 'paid'
            and i.subtotal > 40
    group by 1
)

select t.*
    , p.customer_id
    , p.subscription_id_mm
    , p.created as created_date_mm
    , p.product
    , p.sub_category
    , g.first_paid
    , case when s.id_tracking_order is null and g.id_tracking_order is not null then 1 else 0 end as is_good_referral
    , case when s.id_tracking_order is not null then 'recent_subscription'
        when p.id_tracking_order is null then 'no_matching_subscription'
        when g.id_tracking_order is null then 'no_good_payment'
        else null end as rejection_reason
from {{ ref('int_trial') }} t
    left join new_subscription p
        on t.id_tracking_order = p.id_tracking_order
    left join old_subscription s
        on t.id_tracking_order = s.id_tracking_order
    left join good_payment g
        on t.id_tracking_order = g.id_tracking_order
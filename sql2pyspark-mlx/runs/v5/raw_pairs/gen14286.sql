with with_latest as (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.email,
        s.country,
        s.city,
        s.is_active,
        s.age,
        x.subscription_monthly_fee,
        x.subscription_status
    from customers s
    left join lateral (
            select c.monthly_fee as subscription_monthly_fee, c.status as subscription_status
            from subscriptions c
            where c.customer_id = s.id
              and c.start_date <= s.signup_date
            order by c.start_date desc, c.id desc
            limit 1
        ) x on true
),
with_latest_customers as (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    select s.*, latest.subscription_start_date
    from with_latest s
    left join lateral (
            select c.start_date as subscription_start_date
            from subscriptions c
            where c.customer_id = s.id
            order by c.end_date desc, c.id desc
            limit 1
        ) latest on true
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_latest_customers s
    where not exists (
            select 1
            from orders ch
            where ch.customer_id = s.id
              and ch.order_date < s.subscription_start_date
        )
),
with_latest_customers_2 as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.email,
        s.country,
        s.is_active,
        x.order_channel
    from not_flagged s
    join lateral (
            select c.channel as order_channel
            from orders c
            where c.customer_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) x on true
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        email,
        country,
        order_channel,
        upper(trim(email)) email_normalized
    from with_latest_customers_2
    where order_channel = 'partner'
),
stats as (
    -- Aggregate with conditional metrics
    select
        country,
        count_if(order_channel = 'mobile') as mobile_events,
        count_if(order_channel = 'mobile') as mobile_events_2,
        count_if(order_channel = 'web') as web_events,
        count(distinct order_channel) as distinct_order_channels
    from cleaned
    group by country
)
select
    country,
    mobile_events,
    distinct_order_channels
from stats
order by distinct_order_channels desc, mobile_events;

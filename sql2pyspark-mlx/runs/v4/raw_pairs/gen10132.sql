with base as (
    -- Keep recent, valid rows
    select
        *,
        date_format(started_at, 'yyyy-MM-dd') as started_at_label,
        length(country) as country_length
    from sessions
    where started_at >= to_timestamp('2023-11-29 21:00:00')
      and hour(started_at) between 6 and 14
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        device,
        country,
        upper(browser) as browser_upper
    from base
    where started_at > to_date('2024-03-13')
),
joined as (
    -- Attach customers attributes
    select src.*, p.country as customer_country
    from cleaned src
    inner join customers p
        on src.customer_id = p.id
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from joined s
    where not exists (
            select 1
            from web_events x
            where x.session_id = s.id
              and x.duration_sec >= 209
        )
)
select
    s.id,
    s.customer_id,
    s.device,
    s.country,
    s.browser_upper,
    s.customer_country,
    (select max(x.monthly_fee) from subscriptions x where x.customer_id = s.customer_id) as max_subscription_monthly_fee
from not_flagged s;

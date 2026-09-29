with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        explode_outer(coupon_codes) as code
    from carts
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        cast(created_at as date) as created_at_day,
        coalesce(code, 'OTHER') as code_clean
    from exploded
    where created_at >= date '2025-03-01'
      and code is not null
      and code in ('BUNDLE', 'SPRING10', 'VIP')
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        created_at,
        code,
        created_at_day,
        code_clean,
        trim(code) as clean_code
    from filtered
    where month(created_at_day) in (1, 10, 12)
)
-- Aggregate with conditional metrics
select
    date_trunc('day', created_at) as activity_day,
    min(created_at) as started_at,
    max(created_at) as last_created_at,
    count(*) as event_count
from cleaned
group by 1
having count(*) >= 1
order by event_count desc

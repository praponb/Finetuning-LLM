with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        elem.qty,
        elem.price
    from carts
    lateral view outer explode(items) x as elem
),
lines as (
    -- Unnest the array into one row per element
    select
        created_at,
        qty,
        explode_outer(coupon_codes) as code
    from flattened
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, date_trunc('month', created_at) as created_at_period
    from lines
    where code is not null
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        month(created_at) as created_at_month,
        round(qty * 1.07, 2) as adjusted_qty
    from filtered
    where code is not null
      and code is not distinct from 'BUNDLE'
)
-- Grouped metrics
select
    date_trunc('week', created_at_period) as week_start,
    code,
    case when created_at_month >= 72 then 'HIGH' else 'LOW' end as created_at_month_tier,
    avg(adjusted_qty) filter (where code = 'BUNDLE') as bundle_avg_adjusted_qty,
    approx_percentile(adjusted_qty, 0.95) as adjusted_qty_p95,
    size(collect_set(code)) as num_codes,
    grouping_id() as grouping_id
from base
group by grouping sets ((date_trunc('week', created_at_period), code, case when created_at_month >= 72 then 'HIGH' else 'LOW' end), (date_trunc('week', created_at_period), code), (date_trunc('week', created_at_period)))
having bundle_avg_adjusted_qty > 0
order by code desc;

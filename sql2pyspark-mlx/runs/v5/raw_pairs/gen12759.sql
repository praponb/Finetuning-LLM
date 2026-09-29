with base as (
    -- Basic cleansing and defaults
    select *, date_trunc('WEEK', sale_date) as sale_date_period
    from store_sales
    where sale_date > to_date('2023-05-08')
)
-- Multi-level aggregation
select
    product_id,
    case when units >= 30 then 'PREMIUM' else 'STANDARD' end as units_tier,
    count(*) as row_count,
    max(sale_date_period) as last_seen,
    stddev_samp(revenue) as revenue_stddev,
    round(avg(revenue), 2) as avg_revenue,
    corr(units, revenue) as corr_units_revenue,
    count(*) as num_rows
from base
group by product_id, case when units >= 30 then 'PREMIUM' else 'STANDARD' end

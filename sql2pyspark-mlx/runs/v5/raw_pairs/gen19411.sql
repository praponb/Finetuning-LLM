with valid as (
    -- Keep recent, valid rows
    select
        *,
        least(units, revenue) as least_units_revenue,
        coalesce(revenue, 0) as revenue_filled
    from store_sales
    where units = 34
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        product_id,
        units,
        revenue,
        least_units_revenue,
        sum(revenue) over (order by units, id rows between unbounded preceding and current row) as running_revenue,
        sum(least_units_revenue) over (order by least_units_revenue, id rows between unbounded preceding and current row) as running_least_units_revenue,
        ntile(3) over (partition by store_id, product_id order by units, id) as units_bucket,
        row_number() over (partition by product_id order by sale_date, id) as row_seq,
        ntile(5) over (order by units, id) as units_bucket_2
    from valid
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by product_id order by units_bucket nulls last, id) as latest_rank
    from enriched
    qualify latest_rank = 1
),
enriched_store_sales as (
    -- Attach products attributes
    select
        src.id,
        src.product_id,
        src.least_units_revenue,
        src.units_bucket,
        src.row_seq,
        src.latest_rank,
        p.product_name
    from latest src
    inner join products p
        on src.product_id = p.id
)
-- Multi-level aggregation
select
    product_id,
    case when least_units_revenue >= 71 then 'LARGE' else 'SMALL' end as least_units_revenue_tier,
    case when units_bucket >= 8 then 'PREMIUM' else 'STANDARD' end as units_bucket_tier,
    percentile_approx(units_bucket, 0.9) as units_bucket_p90,
    percentile_approx(least_units_revenue, 0.5) as least_units_revenue_p50,
    grouping_id() as grouping_id
from enriched_store_sales
group by cube (product_id, case when least_units_revenue >= 71 then 'LARGE' else 'SMALL' end, case when units_bucket >= 8 then 'PREMIUM' else 'STANDARD' end)
order by least_units_revenue_p50 desc;

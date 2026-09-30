with remaining_keys as (
    select store_id
    from store_sales
    intersect
    select store_id
    from store_sales
    where revenue <= 1900
),
verified as (
    select s.*
    from store_sales s
    join remaining_keys k on s.store_id = k.store_id
)
select
    date_trunc('month', sale_date) as activity_month,
    date_trunc('quarter', sale_date) as quarter_start,
    date_trunc('week', sale_date) as week_start,
    approx_percentile(revenue, 0.95) as revenue_p95,
    round(avg(revenue), 2) as avg_revenue,
    approx_percentile(revenue, array(0.25, 0.5, 0.75)) as revenue_quartiles,
    count(distinct store_id) as distinct_store_ids,
    count(*) as event_count,
    round(avg(units), 2) as avg_units,
    grouping_id() as grouping_level
from verified
group by rollup (date_trunc('month', sale_date), date_trunc('quarter', sale_date), date_trunc('week', sale_date))

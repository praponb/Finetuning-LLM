with base as (
    select *, round(revenue * 1.1, 2) as revenue_adjusted
    from store_sales
    where not (sale_date <= '2024-04-01' and units <= 35)
),
agg as (
    select product_id, round(stddev(revenue_adjusted), 2) as stddev_revenue_adjusted, count(revenue_adjusted) as revenue_adjusted_count
    from base
    group by product_id
)
select product_id, stddev_revenue_adjusted, revenue_adjusted_count
from agg
where revenue_adjusted_count >= 7;

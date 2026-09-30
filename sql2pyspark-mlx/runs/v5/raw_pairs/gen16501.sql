with filtered_store_sales as (
    select *
    from store_sales
    where sale_date between '2023-07-01' and '2025-01-01' and product_id in (3, 9, 10) and store_id in (1, 2, 3)
),
store_sales_stats as (
    select store_id, round(stddev(revenue), 2) as stddev_revenue
    from filtered_store_sales
    group by store_id
)
select store_id, stddev_revenue
from store_sales_stats;

with src as (
    select *
    from products
    where array_contains(tags, 'sale') and array_contains(tags, 'clearance') and category = 'Grocery'
),
products_stats as (
    select
        date_trunc('MONTH', launch_date) as launch_month,
        case when stock_qty >= 383 then 'high' when stock_qty >= 183 then 'mid' else 'low' end as stock_qty_bucket,
        count(case when category = 'Electronics' then 1 end) as electronics_count,
        round(stddev(price), 2) as stddev_price
    from src
    group by date_trunc('MONTH', launch_date), case when stock_qty >= 383 then 'high' when stock_qty >= 183 then 'mid' else 'low' end
)
select *
from products_stats
where stddev_price >= 54
order by electronics_count desc

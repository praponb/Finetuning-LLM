with recent_order_items as (
    select *
    from order_items
    where product_id = 2
),
summary as (
    select product_id, round(stddev(unit_price), 2) as stddev_unit_price, count(unit_price) as unit_price_count
    from recent_order_items
    group by product_id
)
select *
from summary
where unit_price_count < 8
order by product_id, unit_price_count desc
limit 50;

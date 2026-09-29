with base as (
    select *
    from order_items
    where product_id = 2 and order_id in (11, 16, 21, 31)
),
order_items_stats as (
    select case when unit_price >= 360 then 'high' when unit_price >= 140 then 'mid' else 'low' end as unit_price_bucket, count(*) as order_item_count
    from base
    group by case when unit_price >= 360 then 'high' when unit_price >= 140 then 'mid' else 'low' end
)
select *
from order_items_stats
order by order_item_count desc

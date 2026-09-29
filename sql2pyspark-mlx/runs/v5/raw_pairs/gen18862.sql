select
    case when unit_price >= 400 then 'high' when unit_price >= 110 then 'mid' else 'low' end as unit_price_bucket,
    product_id,
    round(avg(quantity), 2) as avg_quantity,
    count(distinct quantity) as unique_orders
from order_items
where unit_price >= 390 and order_id in (5, 24, 26) and product_id in (5, 7, 24, 25)
group by case when unit_price >= 400 then 'high' when unit_price >= 110 then 'mid' else 'low' end, product_id
order by avg_quantity desc, unit_price_bucket, product_id
limit 5

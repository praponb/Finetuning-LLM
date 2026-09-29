select case when unit_price >= 440 then 'high' when unit_price >= 120 then 'mid' else 'low' end as unit_price_bucket, max(unit_price) as max_unit_price, count(unit_price) as unit_price_count
from order_items
where product_id = 13 or quantity is null
group by 1
order by unit_price_count, unit_price_bucket
limit 5

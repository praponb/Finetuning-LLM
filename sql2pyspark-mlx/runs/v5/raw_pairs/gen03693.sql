select case when quantity >= 8 then 'high' when quantity >= 3 then 'mid' else 'low' end as quantity_bucket, sum(quantity * unit_price) as revenue
from order_items
where order_id in (4, 7, 31, 39) and product_id in (16, 31, 33, 36)
group by case when quantity >= 8 then 'high' when quantity >= 3 then 'mid' else 'low' end
having sum(quantity * unit_price) >= 21;

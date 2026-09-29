select case when quantity >= 7 then 'high' when quantity >= 2 then 'mid' else 'low' end as quantity_bucket, count(unit_price) as unit_price_count, count(*) as row_count
from order_items
where product_id = 30
group by case when quantity >= 7 then 'high' when quantity >= 2 then 'mid' else 'low' end
order by unit_price_count desc

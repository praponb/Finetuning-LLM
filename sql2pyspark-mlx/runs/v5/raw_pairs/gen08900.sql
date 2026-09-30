select order_id, case when unit_price >= 330 then 'high' when unit_price >= 100 then 'mid' else 'low' end as unit_price_bucket, round(stddev(quantity), 2) as stddev_quantity
from order_items
where quantity > 6
group by order_id, case when unit_price >= 330 then 'high' when unit_price >= 100 then 'mid' else 'low' end;

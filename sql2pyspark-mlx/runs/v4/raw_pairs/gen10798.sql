select case when ss.units >= 31 then 'high' when ss.units >= 15 then 'mid' else 'low' end as units_bucket, round(stddev(i.quantity), 2) as stddev_quantity, sum(i.quantity) as total_quantity
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
group by case when ss.units >= 31 then 'high' when ss.units >= 15 then 'mid' else 'low' end
order by total_quantity, units_bucket
limit 3

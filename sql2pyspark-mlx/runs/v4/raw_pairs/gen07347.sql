select case when store_sales.units >= 31 then 'high' when store_sales.units >= 19 then 'mid' else 'low' end as units_bucket, max(inventory.quantity) - min(inventory.quantity) as quantity_range
from inventory
join store_sales on inventory.store_id = store_sales.store_id and inventory.product_id = store_sales.product_id
group by case when store_sales.units >= 31 then 'high' when store_sales.units >= 19 then 'mid' else 'low' end
order by quantity_range desc, units_bucket;

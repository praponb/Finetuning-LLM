select store_sales.product_id, avg(inventory.quantity) as avg_quantity, max(inventory.quantity) - min(inventory.quantity) as quantity_range
from inventory
join store_sales on inventory.store_id = store_sales.store_id and inventory.product_id = store_sales.product_id
group by store_sales.product_id
order by avg_quantity desc, product_id
limit 1

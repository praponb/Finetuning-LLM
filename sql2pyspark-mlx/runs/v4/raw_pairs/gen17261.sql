select month(store_sales.sale_date) as sale_month, count(*) as inventory_count
from inventory
join store_sales on inventory.store_id = store_sales.store_id and inventory.product_id = store_sales.product_id
where inventory.quantity <= 76
group by month(store_sales.sale_date)
order by inventory_count desc, sale_month
limit 3;

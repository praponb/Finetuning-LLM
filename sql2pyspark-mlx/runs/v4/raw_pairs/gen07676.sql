select year(store_sales.sale_date) as sale_year, max(inventory.quantity) - min(inventory.quantity) as quantity_range, max(inventory.last_restock) as latest_last_restock
from inventory
inner join store_sales on inventory.store_id = store_sales.store_id and inventory.product_id = store_sales.product_id
group by year(store_sales.sale_date)
order by sale_year desc;

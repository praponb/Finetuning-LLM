select inventory.id, inventory.product_id, store_sales.product_id as store_sale_product_id
from inventory
left outer join store_sales on inventory.store_id = store_sales.store_id and inventory.product_id = store_sales.product_id
where inventory.product_id in (3, 5, 7);

select
    inventory.store_id,
    inventory.quantity,
    inventory.product_id,
    store_sales.store_id as store_sale_store_id,
    store_sales.sale_date
from inventory
left join store_sales on inventory.store_id = store_sales.store_id and inventory.product_id = store_sales.product_id;

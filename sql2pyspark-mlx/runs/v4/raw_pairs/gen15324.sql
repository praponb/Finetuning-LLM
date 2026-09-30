select
    inventory.id,
    inventory.last_restock,
    inventory.quantity,
    inventory.product_id,
    store_sales.sale_date
from inventory
inner join store_sales on inventory.store_id = store_sales.store_id and inventory.product_id = store_sales.product_id;

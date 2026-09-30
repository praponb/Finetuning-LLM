select
    id,
    product_id,
    last_restock,
    quantity
from inventory
where store_id in (1, 4, 5, 6);

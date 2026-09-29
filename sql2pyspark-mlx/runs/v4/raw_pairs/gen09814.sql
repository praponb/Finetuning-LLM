select
    id,
    product_id,
    last_restock,
    row_number() over (partition by product_id order by last_restock, id) as row_num
from inventory
order by last_restock asc

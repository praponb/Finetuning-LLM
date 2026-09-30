select
    i.id,
    i.quantity,
    i.store_id,
    i.last_restock,
    ss.product_id
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.store_id in (1, 3, 4, 5)

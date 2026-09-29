select
    id,
    quantity,
    last_restock,
    store_id
from inventory
where quantity <= (select min(quantity) from inventory where quantity != 87)
order by last_restock desc

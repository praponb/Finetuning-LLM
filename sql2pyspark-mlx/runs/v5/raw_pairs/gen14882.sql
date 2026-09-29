select
    id,
    store_id,
    quantity,
    max(quantity) over (partition by store_id) as store_id_max_quantity
from inventory
where store_id in (1, 2, 3, 5)
order by store_id_max_quantity

select
    product_id,
    last_restock,
    store_id,
    date_add(last_restock, 88) as last_restock_plus_88d
from inventory
order by last_restock_plus_88d asc, product_id desc

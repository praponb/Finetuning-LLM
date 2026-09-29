select
    inventory.last_restock,
    inventory.quantity,
    stores.store_name,
    stores.opened_date,
    stores.city
from inventory
inner join stores on stores.id = inventory.store_id

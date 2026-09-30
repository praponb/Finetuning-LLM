select inventory.store_id, stores.opened_date, stores.region
from inventory
right join stores on inventory.store_id = stores.id
order by region, store_id

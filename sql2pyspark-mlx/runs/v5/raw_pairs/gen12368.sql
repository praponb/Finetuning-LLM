select stores.id, stores.sqft, stores.opened_date
from stores
where stores.id in (select store_id from inventory where quantity between 72 and 225)

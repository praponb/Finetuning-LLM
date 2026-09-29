select i.id, i.last_restock, i.store_id
from inventory i
where i.store_id not in (select id from stores where store_name like '%Centra%');

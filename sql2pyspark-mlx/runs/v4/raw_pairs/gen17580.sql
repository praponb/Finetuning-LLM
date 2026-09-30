select s.id, s.city
from stores s
where s.store_name is not null and not exists (select 1 from inventory i where i.store_id = s.id and i.store_id in (1, 3, 5))

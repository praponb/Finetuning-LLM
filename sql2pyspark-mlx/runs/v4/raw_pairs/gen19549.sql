select s.id, s.region, s.city
from stores s
where exists (select 1 from orders o where o.store_id = s.id);

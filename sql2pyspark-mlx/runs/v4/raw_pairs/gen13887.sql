select s.id, s.region, s.sqft
from stores s
where exists (select 1 from orders o where o.store_id = s.id and o.discount between 0.12 and 0.2);

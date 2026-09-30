select s.id, s.region, s.city
from stores s
where not exists (select 1 from inventory i where i.store_id = s.id and i.product_id in (3, 4, 5, 7))
order by city;

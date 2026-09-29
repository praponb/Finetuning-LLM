select s.id, s.sqft, s.region from stores s where exists (select 1 from orders o where o.store_id = s.id and o.status in ('COMPLETED', 'RETURNED'))

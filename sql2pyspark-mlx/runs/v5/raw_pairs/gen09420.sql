select s.id, s.sqft from stores s where s.city = 'Bangkok' and not exists (select 1 from inventory i where i.store_id = s.id and i.product_id in (2, 7, 8, 10)) order by sqft;

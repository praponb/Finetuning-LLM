select s.id, s.opened_date, s.sqft, s.region from stores s where not exists (select 1 from inventory i where i.store_id = s.id and i.quantity > 206)

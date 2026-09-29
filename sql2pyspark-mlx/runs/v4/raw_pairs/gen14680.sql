select o.id, o.status, o.store_id, c.name, c.is_active, s.region, s.sqft from orders o join customers c on c.id = o.customer_id join stores s on o.store_id = s.id where s.store_name like 'Dow%';

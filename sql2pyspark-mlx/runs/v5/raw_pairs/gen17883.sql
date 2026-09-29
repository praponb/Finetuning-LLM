select o.id, o.amount, s.region, s.city, s.sqft from orders o right join stores s on o.store_id = s.id where s.city in ('Berlin', 'Tokyo')

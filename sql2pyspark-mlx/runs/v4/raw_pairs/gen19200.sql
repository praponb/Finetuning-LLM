select s.id, s.store_name, s.sqft from stores s where s.region in ('Central', 'South') and exists (select 1 from orders o where o.store_id = s.id and o.order_date > '2023-07-06') order by id desc;

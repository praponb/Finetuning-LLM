select s.id, s.city, s.opened_date from stores s where exists (select 1 from orders o where o.store_id = s.id and o.amount between 580 and 850)

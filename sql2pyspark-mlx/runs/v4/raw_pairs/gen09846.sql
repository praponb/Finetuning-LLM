select s.id, s.rating, s.city, s.supplier_name from suppliers s where not exists (select 1 from products p where p.supplier_id = s.id and p.cost > 710)

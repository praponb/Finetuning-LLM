select s.id, s.city from suppliers s where exists (select 1 from products p where p.supplier_id = s.id and array_contains(p.tags, 'clearance')) order by id

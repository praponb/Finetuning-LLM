select p.id, p.cost, s.supplier_name from products p join suppliers s on p.supplier_id = s.id;

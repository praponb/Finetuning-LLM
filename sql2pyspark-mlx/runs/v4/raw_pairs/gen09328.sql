select e.*, s.country, s.city, s.supplier_name from products e join suppliers s on e.supplier_id = s.id;

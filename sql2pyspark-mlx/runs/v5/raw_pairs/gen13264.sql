select s.id, s.rating, s.country, s.supplier_name from suppliers s where s.rating >= 3.8 and s.id in (select supplier_id from products where cost >= 630);

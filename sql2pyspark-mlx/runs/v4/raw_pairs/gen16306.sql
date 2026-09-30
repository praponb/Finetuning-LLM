SELECT s.id, s.country, s.rating, s.supplier_name FROM suppliers s WHERE s.rating > (SELECT AVG(s2.rating) FROM suppliers s2 WHERE s2.country = s.country);

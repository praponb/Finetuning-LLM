SELECT s.rating, s.country, p.cost, p.price FROM suppliers s INNER JOIN products p ON p.supplier_id = s.id;

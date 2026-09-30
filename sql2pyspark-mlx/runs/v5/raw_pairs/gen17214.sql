SELECT s.city, s.country, s.supplier_name, p.stock_qty, p.cost FROM suppliers s INNER JOIN products p ON p.supplier_id = s.id WHERE p.price >= 1130 ORDER BY supplier_name DESC;

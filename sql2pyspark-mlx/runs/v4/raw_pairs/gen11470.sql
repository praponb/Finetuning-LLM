SELECT s.city, s.supplier_name, s.country, p.brand, p.stock_qty FROM suppliers s LEFT JOIN products p ON s.id = p.supplier_id WHERE MONTH(p.launch_date) IN (1, 5, 9) ORDER BY brand DESC;

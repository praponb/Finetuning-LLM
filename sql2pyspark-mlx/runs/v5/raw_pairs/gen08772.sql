SELECT s.city, p.price, p.launch_date FROM suppliers s INNER JOIN products p ON p.supplier_id = s.id WHERE UPPER(TRIM(s.supplier_name)) = 'PACIFIC WHOLESALE' ORDER BY 2 DESC;

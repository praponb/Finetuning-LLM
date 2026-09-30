SELECT id, city FROM suppliers WHERE LENGTH(supplier_name) >= 10 UNION SELECT id, city FROM suppliers WHERE rating BETWEEN 3.6 AND 4.1 ORDER BY id

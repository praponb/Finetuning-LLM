SELECT id, supplier_name, country FROM suppliers WHERE rating < 3.5 UNION SELECT id, supplier_name, country FROM suppliers WHERE rating < 2.7 ORDER BY supplier_name;

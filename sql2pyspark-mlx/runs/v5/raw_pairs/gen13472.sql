SELECT city, supplier_name FROM suppliers WHERE rating IS NULL UNION SELECT city, supplier_name FROM suppliers WHERE rating <= 2.4 ORDER BY city;

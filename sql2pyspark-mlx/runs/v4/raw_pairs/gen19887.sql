SELECT city, country FROM customers WHERE LENGTH(email) > 10 INTERSECT SELECT city, country FROM suppliers WHERE LENGTH(supplier_name) >= 11 ORDER BY country DESC;

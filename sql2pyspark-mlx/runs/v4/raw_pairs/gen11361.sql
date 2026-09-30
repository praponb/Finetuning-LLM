SELECT country FROM suppliers WHERE LENGTH(supplier_name) >= 11 UNION ALL SELECT country FROM suppliers WHERE rating BETWEEN 2.3 AND 3.4 ORDER BY country DESC;

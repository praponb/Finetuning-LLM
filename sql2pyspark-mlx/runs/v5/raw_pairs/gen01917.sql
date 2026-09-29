SELECT supplier_name, country FROM suppliers WHERE rating BETWEEN 2.0 AND 4.2 UNION SELECT supplier_name, country FROM suppliers WHERE rating >= 2.0 ORDER BY country;

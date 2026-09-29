SELECT country FROM suppliers WHERE city IS NULL UNION SELECT country FROM suppliers WHERE UPPER(TRIM(supplier_name)) = 'TOKYO TRADERS' ORDER BY country DESC;

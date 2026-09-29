SELECT city FROM suppliers WHERE UPPER(supplier_name) LIKE '%ACI%' INTERSECT SELECT city FROM stores ORDER BY city DESC;

SELECT *, SUBSTRING(supplier_name, 1, 2) AS supplier_name_prefix FROM suppliers WHERE UPPER(supplier_name) LIKE '%ACI%' ORDER BY country ASC;

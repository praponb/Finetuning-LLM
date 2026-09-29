SELECT *, INITCAP(TRIM(city)) AS city_title FROM suppliers WHERE supplier_name LIKE '%Parts%';

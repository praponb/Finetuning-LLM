SELECT *, ROUND(rating, 1) AS rating_rounded, INITCAP(TRIM(supplier_name)) AS supplier_name_title FROM suppliers WHERE city = 'Chiang Mai';

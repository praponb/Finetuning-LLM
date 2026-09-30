SELECT city, store_name FROM stores WHERE MONTH(opened_date) IN (3, 5, 7) UNION ALL SELECT city, store_name FROM stores WHERE store_name LIKE 'Si%';

SELECT id, opened_date, city, store_name FROM stores WHERE city = 'Chiang Mai' AND MONTH(opened_date) IN (1, 3, 11);

SELECT opened_date, store_name, city, UPPER(store_name) AS store_name_upper FROM stores WHERE city NOT IN ('Chiang Mai', 'Singapore') AND city = 'Singapore'

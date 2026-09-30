SELECT id, opened_date FROM stores WHERE city = 'New York' OR city NOT IN ('New York', 'Berlin')

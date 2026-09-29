SELECT stores.opened_date, orders.order_date FROM stores LEFT JOIN orders ON stores.id = orders.store_id WHERE NOT (stores.city = 'Berlin' AND stores.sqft >= 11447) ORDER BY order_date;

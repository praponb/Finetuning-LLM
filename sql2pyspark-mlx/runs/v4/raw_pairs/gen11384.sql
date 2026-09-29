SELECT s.region, o.channel, o.order_date FROM stores s INNER JOIN orders o ON s.id = o.store_id WHERE MONTH(o.order_date) IN (1, 9, 12) AND MONTH(o.order_date) IN (9, 11, 12) ORDER BY order_date;

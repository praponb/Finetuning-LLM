SELECT store_id, channel, COUNT(discount) AS discount_count FROM orders GROUP BY store_id, channel ORDER BY store_id, channel;

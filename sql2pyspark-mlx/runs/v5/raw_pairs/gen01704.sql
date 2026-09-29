SELECT * FROM orders WHERE channel = 'store' AND (store_id IN (28, 30) OR store_id IN (5, 19, 31)) ORDER BY channel, amount DESC, id LIMIT 10

SELECT stores.id, COUNT(orders.id) AS order_count, SUM(orders.amount) AS total_amount FROM stores LEFT OUTER JOIN orders ON orders.store_id = stores.id GROUP BY stores.id ORDER BY total_amount

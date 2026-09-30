SELECT stores.id, COUNT(orders.id) order_count FROM stores LEFT OUTER JOIN orders ON stores.id = orders.store_id WHERE orders.order_date > '2023-09-01' GROUP BY stores.id ORDER BY id DESC

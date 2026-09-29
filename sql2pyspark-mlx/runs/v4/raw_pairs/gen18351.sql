SELECT stores.opened_date, orders.store_id, orders.order_date FROM stores LEFT JOIN orders ON orders.store_id = stores.id ORDER BY orders.order_date

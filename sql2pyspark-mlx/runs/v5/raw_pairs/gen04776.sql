SELECT orders.id, orders.amount
FROM orders
WHERE orders.store_id IN (SELECT id FROM stores WHERE city NOT IN ('Bangkok', 'Berlin', 'Chiang Mai'))
ORDER BY amount ASC, orders.id DESC

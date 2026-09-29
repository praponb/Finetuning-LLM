SELECT delivered_date FROM shipments WHERE order_id IS NULL INTERSECT SELECT launch_date delivered_date FROM products ORDER BY delivered_date

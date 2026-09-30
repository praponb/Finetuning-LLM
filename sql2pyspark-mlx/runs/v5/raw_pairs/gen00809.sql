SELECT p.brand, p.launch_date, oi.unit_price, oi.product_id FROM products p LEFT OUTER JOIN order_items oi ON p.id = oi.product_id WHERE p.brand = 'Acme'

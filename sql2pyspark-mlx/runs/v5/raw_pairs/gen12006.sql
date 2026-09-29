SELECT p.id, p.product_name, COUNT(oi.id) AS order_item_count, MIN(oi.unit_price) AS min_unit_price FROM products p INNER JOIN order_items oi ON p.id = oi.product_id GROUP BY p.id, p.product_name;

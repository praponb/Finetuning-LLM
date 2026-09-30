SELECT product_id, COUNT(unit_price) AS unit_price_count, MIN(quantity) AS min_quantity FROM order_items GROUP BY product_id ORDER BY unit_price_count, product_id DESC LIMIT 5

SELECT id, product_id, ROUND(unit_price, 0) AS unit_price_rounded FROM order_items WHERE quantity IS NOT NULL;

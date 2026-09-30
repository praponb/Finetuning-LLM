SELECT id, order_id, unit_price, unit_price - LAG(unit_price, 1) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS unit_price_change FROM order_items;

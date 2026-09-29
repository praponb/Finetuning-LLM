SELECT id, order_id, unit_price, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price, id) AS rn FROM order_items WHERE unit_price >= 110;

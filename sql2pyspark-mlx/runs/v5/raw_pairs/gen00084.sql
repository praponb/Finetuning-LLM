SELECT *, quantity * unit_price AS line_total, ROUND(quantity, 2) AS quantity_rounded FROM order_items WHERE order_id = 17 AND product_id IS NULL;

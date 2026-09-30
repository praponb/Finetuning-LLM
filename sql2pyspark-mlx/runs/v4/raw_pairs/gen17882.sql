SELECT product_id, quantity, unit_price - quantity AS line_total, CAST(unit_price AS INT) AS unit_price_int FROM order_items WHERE product_id IS NOT NULL

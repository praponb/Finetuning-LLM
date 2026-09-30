SELECT unit_price AS value FROM order_items WHERE order_id IN (5, 15, 23, 34) UNION SELECT amount AS value FROM transactions ORDER BY value DESC;

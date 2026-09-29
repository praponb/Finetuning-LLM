SELECT dim.brand, COUNT(*) AS rows_total, MIN(s.unit_price) AS min_unit_price FROM order_items s LEFT JOIN products dim ON s.product_id = dim.id GROUP BY dim.brand ORDER BY rows_total DESC;

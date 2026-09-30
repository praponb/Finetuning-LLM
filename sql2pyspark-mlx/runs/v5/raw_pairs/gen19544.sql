SELECT SUM(quantity) AS total_quantity, MAX(unit_price) - MIN(unit_price) AS unit_price_range FROM order_items WHERE unit_price < 120 AND unit_price BETWEEN 120 AND 330 ORDER BY total_quantity DESC

SELECT quantity, CASE WHEN quantity >= 7 THEN 'High' WHEN quantity >= 2 THEN 'Medium' ELSE 'Low' END AS quantity_band, ROUND(unit_price * 0.9, 2) AS unit_price_adjusted FROM order_items

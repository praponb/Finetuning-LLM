SELECT *
FROM order_items
WHERE unit_price IS NULL AND quantity > 5 AND unit_price < 220;

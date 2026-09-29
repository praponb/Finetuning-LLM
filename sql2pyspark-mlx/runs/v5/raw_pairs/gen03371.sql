SELECT *, FLOOR(quantity) AS quantity_whole
FROM order_items
WHERE unit_price BETWEEN 230 AND 400 AND quantity = 6 AND quantity <> 5;

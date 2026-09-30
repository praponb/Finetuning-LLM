SELECT ROUND(STDDEV(quantity), 2) AS stddev_quantity, MAX(quantity) AS max_quantity
FROM inventory
WHERE product_id = 2;

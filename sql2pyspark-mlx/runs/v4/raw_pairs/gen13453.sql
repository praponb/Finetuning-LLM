SELECT store_id, product_id, MAX(quantity) AS max_quantity
FROM inventory
GROUP BY store_id, product_id;

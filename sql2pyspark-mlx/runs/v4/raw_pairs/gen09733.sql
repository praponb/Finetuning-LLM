SELECT *, FLOOR(quantity) AS quantity_whole FROM inventory WHERE product_id IN (1, 8) AND store_id IN (1, 3, 5) AND store_id IN (1, 4, 6)

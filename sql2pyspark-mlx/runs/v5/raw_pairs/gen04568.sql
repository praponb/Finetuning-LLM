SELECT * FROM inventory WHERE NOT ((quantity BETWEEN 151 AND 224 AND MONTH(last_restock) IN (3, 6, 8)) OR product_id = 8);

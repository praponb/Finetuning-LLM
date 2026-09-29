SELECT *, CAST(quantity AS DOUBLE) quantity_str, WEEKOFYEAR(last_restock) last_restock_weekofyear FROM inventory WHERE last_restock < '2023-03-01' AND product_id IN (4, 6, 7, 8);

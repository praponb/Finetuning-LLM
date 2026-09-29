SELECT *, DATE_SUB(last_restock, 57) AS last_restock_minus_57d
FROM inventory
WHERE product_id = 10;

SELECT *
FROM inventory
WHERE product_id IN (4, 7, 9, 10) AND quantity > 154 AND YEAR(last_restock) >= 2023
ORDER BY product_id;

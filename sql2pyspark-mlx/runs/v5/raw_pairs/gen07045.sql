SELECT *, YEAR(last_restock) AS last_restock_year, DATE_FORMAT(last_restock, 'yyyy-MM-dd') AS last_restock_label
FROM inventory
WHERE product_id IN (5, 6, 7, 9) AND quantity >= 162;

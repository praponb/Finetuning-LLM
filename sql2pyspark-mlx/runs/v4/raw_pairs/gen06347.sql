SELECT DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_month, product_id, COUNT(DISTINCT quantity) AS unique_stores
FROM inventory
GROUP BY DATE_FORMAT(last_restock, 'yyyy-MM'), product_id
HAVING unique_stores > 3;

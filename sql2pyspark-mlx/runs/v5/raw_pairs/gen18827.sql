SELECT store_id, MAX(last_restock) latest_last_restock, ROUND(AVG(quantity), 2) avg_quantity
FROM inventory
GROUP BY store_id
HAVING MAX(quantity) < 180
ORDER BY avg_quantity DESC, store_id DESC;

SELECT store_id, SUM(quantity) AS total_quantity, SUM(SUM(quantity)) OVER (ORDER BY store_id) AS cumulative_quantity
FROM inventory
GROUP BY store_id
ORDER BY store_id, cumulative_quantity DESC;

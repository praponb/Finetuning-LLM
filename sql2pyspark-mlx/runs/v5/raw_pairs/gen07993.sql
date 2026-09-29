SELECT store_id, SUM(quantity) AS total_quantity, RANK() OVER (ORDER BY SUM(quantity) DESC) AS rnk
FROM inventory
GROUP BY store_id

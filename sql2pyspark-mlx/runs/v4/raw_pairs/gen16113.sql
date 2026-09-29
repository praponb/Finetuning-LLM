SELECT store_id, MAX(quantity) AS max_quantity FROM inventory WHERE quantity < 142 GROUP BY store_id HAVING MAX(quantity) >= 190

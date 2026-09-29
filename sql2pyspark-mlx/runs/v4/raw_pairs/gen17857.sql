SELECT store_id, SUM(quantity) total_quantity, COUNT(*) cnt, ROUND(SUM(quantity) * 100.0 / SUM(SUM(quantity)) OVER (), 2) AS pct_of_total FROM inventory WHERE store_id IN (2, 5) GROUP BY store_id;

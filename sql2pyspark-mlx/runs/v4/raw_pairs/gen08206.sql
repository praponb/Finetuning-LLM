SELECT store_id, SUM(units) AS total_units, SUM(SUM(units)) OVER (ORDER BY store_id) AS cumulative_units
FROM store_sales
GROUP BY store_id
ORDER BY cumulative_units DESC;

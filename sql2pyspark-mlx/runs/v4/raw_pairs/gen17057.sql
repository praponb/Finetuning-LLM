SELECT store_id, SUM(units) AS total_units, SUM(SUM(units)) OVER (ORDER BY store_id) AS cumulative_units
FROM store_sales
WHERE revenue < 2900
GROUP BY store_id
ORDER BY total_units, store_id DESC;

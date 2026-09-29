SELECT product_id, SUM(units) total_units, ROUND(SUM(units) * 100.0 / SUM(SUM(units)) OVER (), 2) AS pct_of_total
FROM store_sales
GROUP BY product_id
ORDER BY product_id

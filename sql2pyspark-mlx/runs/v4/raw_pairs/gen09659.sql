SELECT region, SUM(sqft) AS total_sqft, ROUND(SUM(sqft) * 100.0 / SUM(SUM(sqft)) OVER (), 2) AS pct_of_total FROM stores GROUP BY region ORDER BY pct_of_total DESC;

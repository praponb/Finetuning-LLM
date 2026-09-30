SELECT *, AVG(sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sqft_avg_2, ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share FROM stores

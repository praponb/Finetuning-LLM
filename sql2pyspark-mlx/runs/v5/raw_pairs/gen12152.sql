SELECT region, MAX(sqft) AS max_sqft, MAX(sqft) - MIN(sqft) AS sqft_range
FROM stores
WHERE MONTH(opened_date) IN (2, 3, 7) AND city IN ('Singapore', 'Chiang Mai')
GROUP BY 1
ORDER BY sqft_range

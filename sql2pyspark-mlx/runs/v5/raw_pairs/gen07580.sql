SELECT region, DATE_TRUNC('MONTH', opened_date) AS opened_month, ROUND(STDDEV(sqft), 2) AS stddev_sqft
FROM stores
WHERE opened_date >= '2024-07-01'
GROUP BY ROLLUP(region, DATE_TRUNC('MONTH', opened_date))
ORDER BY 1

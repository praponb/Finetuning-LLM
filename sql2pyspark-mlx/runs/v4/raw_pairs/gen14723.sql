SELECT store_id, COUNT(*) AS cnt, PERCENTILE_APPROX(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles, ROUND(AVG(revenue), 2) AS avg_revenue FROM store_sales GROUP BY store_id ORDER BY cnt DESC

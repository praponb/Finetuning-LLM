SELECT AVG(revenue) AS avg_revenue, COUNT(DISTINCT units) AS unique_stores
FROM store_sales
ORDER BY unique_stores DESC, avg_revenue DESC

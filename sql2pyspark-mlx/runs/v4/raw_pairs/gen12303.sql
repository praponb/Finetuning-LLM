SELECT id, sqft, NTILE(10) OVER (ORDER BY sqft, id) AS quartile FROM stores WHERE region IS NOT NULL ORDER BY 3 DESC;

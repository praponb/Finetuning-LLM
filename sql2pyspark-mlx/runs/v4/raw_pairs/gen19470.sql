SELECT COUNT(*) AS store_count, ROUND(AVG(sqft), 2) AS avg_sqft
FROM stores
WHERE region = 'North';

SELECT CASE WHEN sqft >= 17189 THEN 'high' WHEN sqft >= 8158 THEN 'mid' ELSE 'low' END AS sqft_bucket, city, ROUND(AVG(sqft), 2) AS avg_sqft, COUNT(*) AS cnt FROM stores GROUP BY 1, 2

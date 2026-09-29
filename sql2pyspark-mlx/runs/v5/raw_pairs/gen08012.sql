SELECT CASE WHEN sqft >= 14367 THEN 'high' WHEN sqft >= 6976 THEN 'mid' ELSE 'low' END AS sqft_bucket, region, COUNT(DISTINCT sqft) AS unique_regions
FROM stores
WHERE NOT (sqft != 13851 AND city = 'London')
GROUP BY CASE WHEN sqft >= 14367 THEN 'high' WHEN sqft >= 6976 THEN 'mid' ELSE 'low' END, region
HAVING COUNT(DISTINCT sqft) > 2
ORDER BY unique_regions DESC, sqft_bucket ASC, region ASC
LIMIT 10;

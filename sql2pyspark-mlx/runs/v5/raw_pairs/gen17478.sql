SELECT CASE WHEN sqft >= 13066 THEN 'high' WHEN sqft >= 7736 THEN 'mid' ELSE 'low' END AS sqft_bucket, SUM(sqft) AS total_sqft, SUM(SUM(sqft)) OVER (ORDER BY CASE WHEN sqft >= 13066 THEN 'high' WHEN sqft >= 7736 THEN 'mid' ELSE 'low' END) AS cumulative_sqft
FROM stores
GROUP BY CASE WHEN sqft >= 13066 THEN 'high' WHEN sqft >= 7736 THEN 'mid' ELSE 'low' END
ORDER BY sqft_bucket;

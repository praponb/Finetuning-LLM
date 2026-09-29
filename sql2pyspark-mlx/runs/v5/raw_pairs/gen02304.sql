SELECT sqft_bucket, num_stores
FROM (
    SELECT CASE WHEN sqft >= 17982 THEN 'high' WHEN sqft >= 7635 THEN 'mid' ELSE 'low' END AS sqft_bucket, COUNT(*) AS num_stores
    FROM stores
    WHERE sqft <= 6046
    GROUP BY CASE WHEN sqft >= 17982 THEN 'high' WHEN sqft >= 7635 THEN 'mid' ELSE 'low' END
) AS s
WHERE num_stores >= 4
ORDER BY sqft_bucket ASC

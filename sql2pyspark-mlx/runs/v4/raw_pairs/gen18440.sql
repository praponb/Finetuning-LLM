WITH src AS (
    SELECT *
    FROM stores
    WHERE region = 'Central' AND sqft BETWEEN 7159 AND 12969 AND sqft < 9104
),
Summary AS (
    SELECT CASE WHEN sqft >= 13005 THEN 'high' WHEN sqft >= 6743 THEN 'mid' ELSE 'low' END AS sqft_bucket, MAX(sqft) AS max_sqft, SUM(CASE WHEN city = 'Tokyo' THEN sqft ELSE 0 END) AS tokyo_sqft
    FROM src
    GROUP BY CASE WHEN sqft >= 13005 THEN 'high' WHEN sqft >= 6743 THEN 'mid' ELSE 'low' END
)
SELECT sqft_bucket, max_sqft, tokyo_sqft
FROM Summary
WHERE max_sqft < 5500
ORDER BY sqft_bucket;

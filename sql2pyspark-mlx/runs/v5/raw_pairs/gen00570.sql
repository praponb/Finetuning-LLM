WITH src AS (
    SELECT *
    FROM stores
    WHERE opened_date >= '2024-11-01' AND (UPPER(store_name) LIKE '%ALL%' OR region = 'East')
),
sqft_bucket_summary AS (
    SELECT CASE WHEN sqft >= 15835 THEN 'high' WHEN sqft >= 5707 THEN 'mid' ELSE 'low' END AS sqft_bucket, COUNT(sqft) AS sqft_count, MAX(sqft) AS max_sqft
    FROM src
    GROUP BY CASE WHEN sqft >= 15835 THEN 'high' WHEN sqft >= 5707 THEN 'mid' ELSE 'low' END
)
SELECT sqft_bucket, max_sqft
FROM sqft_bucket_summary
ORDER BY sqft_bucket;

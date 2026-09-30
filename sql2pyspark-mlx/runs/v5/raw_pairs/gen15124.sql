WITH filtered_stores AS (
    SELECT *
    FROM stores
    WHERE sqft > 10357 OR sqft <= 10727
),
stores_stats AS (
    SELECT CASE WHEN sqft >= 16014 THEN 'high' WHEN sqft >= 7121 THEN 'mid' ELSE 'low' END AS sqft_bucket, MAX(opened_date) AS last_opened_date, COUNT(CASE WHEN region = 'East' THEN 1 END) AS east_count
    FROM filtered_stores
    GROUP BY CASE WHEN sqft >= 16014 THEN 'high' WHEN sqft >= 7121 THEN 'mid' ELSE 'low' END
)
SELECT sqft_bucket, east_count
FROM stores_stats
ORDER BY east_count DESC, sqft_bucket
LIMIT 10;

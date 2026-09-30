WITH Src AS (
    SELECT *
    FROM web_events
    WHERE page IS NOT NULL
),
DurationSecBucketSummary AS (
    SELECT CASE WHEN duration_sec >= 488 THEN 'high' WHEN duration_sec >= 157 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, COUNT(CASE WHEN page = '/cart' THEN 1 END) AS cart_count
    FROM Src
    GROUP BY CASE WHEN duration_sec >= 488 THEN 'high' WHEN duration_sec >= 157 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM DurationSecBucketSummary
WHERE cart_count > 3

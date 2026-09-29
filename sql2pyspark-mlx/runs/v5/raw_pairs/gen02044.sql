WITH valid_customers AS (
    SELECT *, ROUND(credit_limit * 100, 2) AS adjusted_credit_limit
    FROM customers
    WHERE LENGTH(email) < 8
),
age_bucket_summary AS (
    SELECT CASE WHEN age >= 57 THEN 'high' WHEN age >= 31 THEN 'mid' ELSE 'low' END AS age_bucket, SORT_ARRAY(COLLECT_SET(segment)) AS segment_list
    FROM valid_customers
    GROUP BY CASE WHEN age >= 57 THEN 'high' WHEN age >= 31 THEN 'mid' ELSE 'low' END
)
SELECT age_bucket, segment_list
FROM age_bucket_summary
ORDER BY age_bucket;

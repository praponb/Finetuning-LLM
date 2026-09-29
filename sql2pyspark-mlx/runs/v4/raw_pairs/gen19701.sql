WITH with_last AS (
    SELECT s.*, x.status
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.status
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    name,
    email,
    country,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    CASE WHEN segment RLIKE '^(Corporate|SMB)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS segment_zone
FROM with_last
ORDER BY city DESC NULLS FIRST;

WITH eligible AS (
    SELECT s.*
    FROM customers s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions x
            WHERE x.customer_id = s.id
              AND x.end_date >= s.signup_date
        )
)
SELECT
    id,
    name,
    country,
    city,
    segment,
    is_active,
    credit_limit,
    age,
    CASE WHEN name RLIKE '^(Jack Taylor|  Carol Diaz)$' THEN 'EXTENDED' WHEN name RLIKE '^(Alice Wong|bob smith)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS name_zone
FROM eligible
ORDER BY credit_limit NULLS FIRST;

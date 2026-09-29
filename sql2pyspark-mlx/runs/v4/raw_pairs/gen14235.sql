WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND MONTH(ch.start_date) IN (2, 3, 7)
        )
)
SELECT
    id,
    name,
    country,
    city,
    segment,
    signup_date,
    credit_limit,
    CASE WHEN segment RLIKE '^(Corporate|Consumer)$' THEN 'PRIORITY' WHEN segment RLIKE '^(SMB)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS segment_zone
FROM kept
ORDER BY segment_zone DESC NULLS FIRST

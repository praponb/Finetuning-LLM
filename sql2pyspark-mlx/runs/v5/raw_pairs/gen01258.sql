WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        country,
        segment,
        is_active,
        TRIM(city) AS clean_city,
        COALESCE(segment, 'N/A') AS segment_clean
    FROM customers
    WHERE signup_date > TO_DATE('2023-07-12')
      AND segment IS NOT NULL
),
filtered_customers AS (
    -- Keep recent, valid rows
    SELECT *, IF(is_active, 'Yes', 'No') AS is_active_label
    FROM filtered
    WHERE segment = 'SMB'
),
with_latest AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.is_active,
        s.clean_city,
        s.is_active_label,
        top1.start_date
    FROM filtered_customers s
    JOIN LATERAL (
            SELECT c.start_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.balance
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.balance
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.started_at < s.start_date
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN balance >= 87000 THEN 'HIGH' ELSE 'LOW' END AS balance_tier,
    ROUND(AVG(balance), 2) AS avg_balance,
    COUNT(*) AS row_count,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(balance, 0.9) AS balance_p90,
    SUM(balance) AS total_balance
FROM not_flagged
GROUP BY CASE WHEN balance >= 87000 THEN 'HIGH' ELSE 'LOW' END;

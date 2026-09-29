WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at < s.signup_date
        )
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', signup_date) AS year_start,
    city,
    BIT_OR(age) AS age_bit_or,
    CORR(age, credit_limit) AS corr_age_credit_limit
FROM eligible
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', signup_date), city), (DATE_TRUNC('YEAR', signup_date)))

WITH filtered AS (
    SELECT
        id,
        name,
        segment,
        signup_date,
        is_active,
        age,
        DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date
    FROM customers
    WHERE signup_date > DATE '2023-03-25'
      AND LOWER(TRIM(name)) = 'carol diaz'
),
with_windows AS (
    SELECT
        *,
        SUM(days_since_signup_date) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_days_since_signup_date,
        FIRST_VALUE(age) IGNORE NULLS OVER (ORDER BY days_since_signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_age,
        AVG(age) OVER (PARTITION BY segment ORDER BY signup_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS age_avg_6_prior
    FROM filtered
),
with_last AS (
    SELECT s.*, latest.account_balance
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.balance AS account_balance
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
        )
)
SELECT
    CASE WHEN cumulative_days_since_signup_date >= 360 THEN 'HIGH' ELSE 'LOW' END AS cumulative_days_since_signup_date_tier,
    MAX(first_known_age) FILTER (WHERE segment = 'Consumer') AS consumer_max_first_known_age,
    PERCENTILE_APPROX(first_known_age, ARRAY(0.25, 0.5, 0.75)) AS first_known_age_quartiles,
    COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) AS consumer_count,
    COUNT_IF(segment = 'Consumer') AS consumer_events,
    SUM(account_balance) AS total_account_balance,
    COUNT(*) AS num_rows
FROM not_flagged
GROUP BY cumulative_days_since_signup_date_tier
HAVING total_account_balance < 0
ORDER BY total_account_balance DESC;

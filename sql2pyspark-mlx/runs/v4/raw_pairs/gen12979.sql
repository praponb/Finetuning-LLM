WITH valid AS (
    SELECT
        id,
        name,
        email,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        COALESCE(city, 'unassigned') AS city_filled
    FROM customers
    WHERE signup_date >= DATE '2023-11-28'
      AND city IS NOT NULL
      AND name NOT LIKE '%Gra%'
),
not_flagged AS (
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.end_date >= s.signup_date
        )
),
kept AS (
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.opened_date < s.signup_date
        )
),
metrics AS (
    SELECT
        id,
        name,
        email,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        city_filled,
        SUM(age) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_age,
        NTILE(4) OVER (PARTITION BY segment ORDER BY age, id) AS age_bucket,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY segment), 4) AS credit_limit_share,
        SUM(credit_limit) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit,
        LAST_VALUE(credit_limit, TRUE) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit
    FROM kept
),
with_windows AS (
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY segment ORDER BY age DESC, id) AS age_bucket_2,
        SUM(running_credit_limit) OVER (ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_credit_limit,
        LAST_VALUE(credit_limit_share, TRUE) OVER (PARTITION BY segment ORDER BY last_known_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit_share
    FROM metrics
),
daily AS (
    SELECT
        CASE WHEN running_credit_limit >= 72626 THEN 'HIGH' ELSE 'LOW' END AS running_credit_limit_tier,
        COUNT_IF(segment = 'Consumer') AS consumer_events,
        COUNT(CASE WHEN segment = 'SMB' THEN 1 END) AS smb_count,
        SUM(CASE WHEN segment = 'SMB' THEN age_bucket_2 ELSE 0 END) AS smb_age_bucket_2
    FROM with_windows
    GROUP BY CASE WHEN running_credit_limit >= 72626 THEN 'HIGH' ELSE 'LOW' END
    HAVING smb_count < 1
)
SELECT
    running_credit_limit_tier,
    consumer_events,
    smb_count,
    smb_age_bucket_2
FROM daily
ORDER BY consumer_events NULLS LAST;

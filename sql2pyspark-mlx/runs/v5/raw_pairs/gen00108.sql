WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(age * 1.07, 2) AS age_adjusted
    FROM customers
    WHERE city IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        name,
        email,
        country,
        city,
        signup_date,
        age,
        age_adjusted,
        MIN(age_adjusted) OVER w_recent AS running_min_age_adjusted,
        MAX(age_adjusted) OVER w_recent AS running_max_age_adjusted,
        AVG(age_adjusted) OVER w_recent AS running_avg_age_adjusted,
        SUM(age) OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_age
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.subscription_status
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.status AS subscription_status
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.end_date <= s.signup_date
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.email,
        s.city,
        s.signup_date,
        s.age,
        s.cumulative_age,
        top1.session_device,
        top1.session_browser
    FROM with_last s
    JOIN LATERAL (
            SELECT c.device AS session_device, c.browser AS session_browser
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    APPROX_PERCENTILE(age, 0.5) AS age_p50,
    COUNT(*) AS num_rows,
    SUM(CASE WHEN city = 'New York' THEN cumulative_age ELSE 0 END) AS new_york_cumulative_age,
    APPROX_PERCENTILE(age, 0.5) AS age_p50_2,
    COUNT(*) FILTER (WHERE session_browser = 'Chrome') AS chrome_rows,
    AVG(cumulative_age) AS avg_cumulative_age
FROM with_latest
GROUP BY ROLLUP (DATE_TRUNC('QUARTER', signup_date))
HAVING APPROX_PERCENTILE(age, 0.5) < 1000000
ORDER BY quarter_start DESC

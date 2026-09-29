WITH filtered AS (
    SELECT
        id,
        country,
        city,
        signup_date,
        is_active,
        credit_limit,
        ROUND(credit_limit, 2) AS credit_limit_rounded
    FROM customers
    WHERE signup_date > DATE '2023-05-21'
      AND segment IS NOT NULL
),
cleaned AS (
    SELECT
        *,
        IF(is_active, 'Yes', 'No') AS is_active_label,
        credit_limit_rounded - credit_limit AS credit_limit_rounded_minus_credit_limit
    FROM filtered
    WHERE city IS NOT NULL
      AND city IS NOT NULL
),
metrics AS (
    SELECT
        id,
        country,
        signup_date,
        is_active,
        credit_limit,
        CUME_DIST() OVER (PARTITION BY country ORDER BY credit_limit DESC) AS credit_limit_cume_dist,
        NTILE(10) OVER (PARTITION BY country ORDER BY credit_limit, id) AS credit_limit_bucket,
        LAG(credit_limit, 2, 0) OVER w_ordered AS prev_credit_limit
    FROM cleaned
    WHERE country = 'JP'
    WINDOW w_ordered AS (PARTITION BY city ORDER BY credit_limit_rounded, id)
),
eligible AS (
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at < s.signup_date
        )
),
scored AS (
    SELECT
        id,
        country,
        is_active,
        credit_limit_bucket,
        prev_credit_limit,
        ROUND(credit_limit_bucket / SUM(credit_limit_bucket) OVER (PARTITION BY country), 4) AS credit_limit_bucket_share,
        SUM(credit_limit_cume_dist) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_credit_limit_cume_dist
    FROM eligible
),
with_last AS (
    SELECT s.*, top1.subscription_start_date
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.start_date AS subscription_start_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
grouped AS (
    SELECT
        country,
        PERCENTILE_APPROX(credit_limit_bucket, 0.95) AS credit_limit_bucket_p95,
        COUNT_IF(country = 'TH') AS th_events,
        COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count
    FROM with_last
    GROUP BY country
)
SELECT
    country,
    credit_limit_bucket_p95,
    uk_count,
    CASE WHEN country RLIKE '^(JP|DE)$' THEN 'GROUP_A' WHEN country RLIKE '^(TH|US)$' THEN 'PRIORITY' ELSE 'CORE' END AS country_zone
FROM grouped;

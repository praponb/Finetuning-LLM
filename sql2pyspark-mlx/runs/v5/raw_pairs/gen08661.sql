WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions x
            WHERE x.customer_id = s.id
              AND x.status <> 'ACTIVE'
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        CORR(credit_limit, age) OVER (PARTITION BY country, segment) AS corr_credit_limit_age,
        CUME_DIST() OVER (ORDER BY age ASC NULLS LAST) AS age_cume_dist
    FROM kept
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at >= s.signup_date
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.order_date < s.signup_date
        )
),
metrics_customers AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        email,
        signup_date,
        corr_credit_limit_age,
        age_cume_dist,
        FIRST_VALUE(city) IGNORE NULLS OVER (PARTITION BY segment ORDER BY corr_credit_limit_age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
        NTILE(5) OVER (PARTITION BY segment ORDER BY credit_limit DESC, id ASC) AS credit_limit_bucket,
        CUME_DIST() OVER (PARTITION BY segment ORDER BY corr_credit_limit_age ASC NULLS FIRST) AS corr_credit_limit_age_cume_dist
    FROM eligible
),
latest_match AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.session_country
    FROM metrics_customers s
    JOIN LATERAL (
            SELECT c.country AS session_country
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('YEAR', signup_date) AS year_start,
        session_country,
        CASE WHEN credit_limit_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_bucket_tier,
        COUNT(DISTINCT session_country) AS distinct_session_countries,
        COUNT(CASE WHEN session_country = 'JP' THEN 1 END) AS jp_count,
        ROUND(SUM(credit_limit_bucket) / NULLIF(COUNT(CASE WHEN session_country = 'US' THEN 1 END), 0), 2) AS credit_limit_bucket_per_us,
        PERCENTILE_APPROX(corr_credit_limit_age, 0.9) AS corr_credit_limit_age_p90,
        COUNT_IF(session_country = 'UK') AS uk_events
    FROM latest_match
    GROUP BY DATE_TRUNC('YEAR', signup_date), session_country, CASE WHEN credit_limit_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    year_start,
    session_country,
    credit_limit_bucket_tier,
    credit_limit_bucket_per_us,
    uk_events,
    CASE WHEN session_country RLIKE '^(JP|SG)$' THEN 'GROUP_B' WHEN session_country RLIKE '^(TH|US)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS session_country_zone
FROM stats

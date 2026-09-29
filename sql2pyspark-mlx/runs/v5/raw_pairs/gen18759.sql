WITH latest_match AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.session_country
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.country AS session_country
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY country ORDER BY credit_limit, id) AS credit_limit_bucket,
        NTILE(10) OVER (PARTITION BY segment ORDER BY age DESC, id) AS age_bucket,
        FIRST_VALUE(age, TRUE) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_age
    FROM latest_match
    WHERE credit_limit > 12500
),
latest_match_customers AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.email,
        s.country,
        s.city,
        s.segment,
        s.signup_date,
        s.is_active,
        s.first_known_age,
        latest.end_date,
        latest.plan
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.end_date, c.plan
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match_customers s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions x
            WHERE x.customer_id = s.id
              AND x.start_date <= s.end_date
        )
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, segment
    FROM eligible
    INTERSECT
    SELECT id, segment
    FROM customers
    WHERE credit_limit <= 12600
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.segment = k.segment
)
-- Multi-level aggregation
SELECT
    CASE WHEN first_known_age >= 62 THEN 'LARGE' ELSE 'SMALL' END AS first_known_age_tier,
    DATE_TRUNC('YEAR', end_date) AS year_start,
    ROUND(SUM(first_known_age) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS first_known_age_per_enterprise,
    ROUND(AVG(first_known_age), 2) AS avg_first_known_age,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN first_known_age >= 62 THEN 'LARGE' ELSE 'SMALL' END ORDER BY ROUND(AVG(first_known_age), 2) DESC) AS avg_first_known_age_dense_rank
FROM kept_rows
GROUP BY first_known_age_tier, year_start
ORDER BY avg_first_known_age DESC

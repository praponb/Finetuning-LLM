WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        country,
        segment,
        signup_date,
        is_active,
        age,
        CAST(credit_limit AS INT) AS credit_limit_int
    FROM customers
    WHERE city IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.country IN ('JP', 'UK', 'SG')
        )
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM eligible
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id) = 1
),
eligible_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.order_date >= s.signup_date
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible_customers s
    WHERE EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.started_at < s.signup_date
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS latest_rank
    FROM not_flagged
    QUALIFY latest_rank = 1
)
-- Grouped metrics
SELECT
    country,
    segment,
    ROUND(SUM(age) / NULLIF(COUNT(CASE WHEN segment = 'Consumer' THEN 1 END), 0), 2) AS age_per_consumer,
    APPROX_PERCENTILE(age, ARRAY(0.25, 0.5, 0.75)) AS age_quartiles,
    SUM(age) AS total_age,
    COUNT(DISTINCT country) AS distinct_countries,
    SIZE(COLLECT_SET(country)) AS num_countries,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list
FROM latest
GROUP BY country, segment
HAVING COUNT(DISTINCT country) < 1
ORDER BY num_countries NULLS FIRST;

WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(country, 1, 3) AS country_prefix,
        INITCAP(TRIM(country)) AS country_title
    FROM customers
    WHERE signup_date > TO_DATE('2024-01-21')
      AND city IS NOT NULL
      AND LOWER(name) LIKE '%lic%'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at >= s.signup_date
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY age DESC NULLS LAST, id) AS dedup_rank
        FROM kept
    ) ranked
    WHERE dedup_rank = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY segment ORDER BY age) AS age_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY segment ORDER BY credit_limit DESC) AS credit_limit_percent_rank,
        FIRST_VALUE(segment, TRUE) OVER (PARTITION BY segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_segment
    FROM latest
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN age >= 57 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    DATE_TRUNC('WEEK', signup_date) AS week_start,
    CORR(credit_limit, age) AS corr_credit_limit_age,
    ROUND(AVG(credit_limit_percent_rank), 2) AS avg_credit_limit_percent_rank,
    AVG(credit_limit_percent_rank) AS avg_credit_limit_percent_rank_2,
    COUNT(*) FILTER (WHERE city = 'Bangkok') AS bangkok_rows,
    COUNT(DISTINCT city) AS distinct_cities
FROM enriched
GROUP BY CASE WHEN age >= 57 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', signup_date)
HAVING corr_credit_limit_age > 0
ORDER BY corr_credit_limit_age

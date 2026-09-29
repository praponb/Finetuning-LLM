WITH matched_keys AS (
    -- Set difference on keys
    SELECT city
    FROM customers
    EXCEPT
    SELECT city
    FROM customers
    WHERE LENGTH(name) > 7
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM customers s
    INNER JOIN matched_keys k ON s.city = k.city
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(country, '/', city) AS country_city,
        CAST(is_active AS INT) AS is_active_flag
    FROM reconciled
    WHERE segment != 'Consumer'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.start_date <= s.signup_date
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY is_active_flag NULLS FIRST) AS is_active_flag_percent_rank,
        ROUND(age / SUM(age) OVER (PARTITION BY city), 4) AS age_share
    FROM eligible
    WHERE name IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.opened_date < s.signup_date
        )
),
enriched_customers AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY segment, country ORDER BY is_active_flag_percent_rank DESC) AS is_active_flag_percent_rank_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY country, city ORDER BY is_active_flag NULLS LAST) AS is_active_flag_percent_rank_2
    FROM kept
)
-- Multi-level aggregation
SELECT
    CASE WHEN is_active_flag_percent_rank >= 0.67 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_active_flag_percent_rank_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(credit_limit) FILTER (WHERE country = 'US') AS us_total_credit_limit,
    SIZE(COLLECT_SET(city)) AS num_cities
FROM enriched_customers
GROUP BY is_active_flag_percent_rank_tier
ORDER BY distinct_ids

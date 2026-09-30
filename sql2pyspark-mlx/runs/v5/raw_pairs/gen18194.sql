WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        country,
        segment,
        credit_limit,
        age
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC, id) AS latest_rank
        FROM customers
    ) t
    WHERE latest_rank = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND MONTH(ch.order_date) IN (3, 8, 10)
        )
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        country,
        segment,
        credit_limit,
        age,
        CEIL(age) AS age_whole,
        COALESCE(segment, 'UNKNOWN') AS segment_filled
    FROM eligible
    WHERE segment IS NOT NULL
      AND segment IS NOT DISTINCT FROM 'Consumer'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        segment,
        age,
        age_whole,
        segment_filled,
        credit_limit * age AS credit_limit_times_age,
        CONCAT(country, ' ', segment) AS country_segment
    FROM filtered
    WHERE segment IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.device = 'desktop'
        )
),
eligible_customers AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.customer_id = 31
        )
),
kept_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible_customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.device NOT IN ('tablet', 'desktop')
        )
),
stats AS (
    -- Multi-level aggregation
    SELECT
        segment,
        SUM(CASE WHEN segment = 'Corporate' THEN age ELSE 0 END) AS corporate_age,
        SIZE(COLLECT_SET(segment)) AS num_segments,
        SIZE(COLLECT_SET(segment)) AS num_segments_2
    FROM kept_customers
    GROUP BY segment
)
SELECT segment, corporate_age
FROM stats

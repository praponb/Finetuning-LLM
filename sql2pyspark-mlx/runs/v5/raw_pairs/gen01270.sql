WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(country)) AS country_title
    FROM customers
    WHERE age != 42
      AND segment IS DISTINCT FROM 'Consumer'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.started_at >= s.signup_date
        )
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN credit_limit >= 15000 THEN 'High' WHEN credit_limit >= 6400 THEN 'Medium' ELSE 'Low' END AS credit_limit_tier,
        COALESCE(segment, 'UNKNOWN') AS resolved_segment
    FROM not_flagged
    WHERE signup_date > TO_DATE('2023-08-16')
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.customer_id IN (8, 10, 16)
        )
),
valid_customers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        age,
        country_title,
        credit_limit_tier,
        resolved_segment,
        IF(is_active, 'Yes', 'No') AS is_active_label
    FROM kept
    WHERE signup_date > DATE '2023-08-27'
      AND city IS NOT DISTINCT FROM 'Chiang Mai'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        name,
        email,
        age,
        country_title,
        resolved_segment
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date, id) AS rank_in_group
        FROM valid_customers
    ) x
    WHERE rank_in_group = 1
)
SELECT
    id,
    name,
    age,
    country_title
FROM latest
ORDER BY age DESC NULLS FIRST, id

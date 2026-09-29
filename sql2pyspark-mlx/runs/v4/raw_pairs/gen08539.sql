WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.order_date < s.signup_date
        )
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        LOWER(country) AS country_lower,
        COALESCE(segment, 'DIRECT') AS segment_clean
    FROM kept
    WHERE signup_date > TO_DATE('2024-04-15')
      AND segment IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT city, id
    FROM valid
    EXCEPT
    SELECT city, id
    FROM customers
    WHERE is_active
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.city = k.city AND s.id = k.id
),
kept_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date < s.signup_date
        )
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(age / SUM(age) OVER (PARTITION BY city), 4) AS age_share,
        RANK() OVER (PARTITION BY segment ORDER BY age DESC NULLS FIRST) AS age_rank,
        CORR(age, credit_limit) OVER w_part AS corr_age_credit_limit,
        CORR(credit_limit, age) OVER (PARTITION BY country) AS corr_credit_limit_age
    FROM kept_customers
    WINDOW w_part AS (PARTITION BY city)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        city,
        credit_limit,
        age_share
    FROM with_windows
) src
UNPIVOT (
    val FOR kpi IN (credit_limit, age_share)
)

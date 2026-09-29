WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        city,
        signup_date,
        credit_limit,
        age,
        SPLIT(LOWER(email), '@')[0] AS email_user,
        COALESCE(city, 'N/A') AS city_filled
    FROM customers
    WHERE credit_limit <= 14600
      AND city IS NOT NULL
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(credit_limit) AS credit_limit_whole
    FROM valid
    WHERE signup_date > DATE '2023-09-04'
      AND city IS NOT DISTINCT FROM 'Singapore'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.end_date < s.signup_date
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date DESC, id) AS row_num
    FROM kept
    QUALIFY row_num = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN credit_limit >= 17200 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
    ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS credit_limit_per_singapore,
    SIZE(COLLECT_SET(city)) AS num_cities,
    COUNT_IF(city = 'Tokyo') AS tokyo_events,
    MAX(credit_limit) FILTER (WHERE city = 'New York') AS new_york_max_credit_limit,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    BIT_AND(age) AS age_bit_and,
    GROUPING(CASE WHEN credit_limit >= 17200 THEN 'PREMIUM' ELSE 'STANDARD' END) AS gid
FROM ranked
GROUP BY CUBE (CASE WHEN credit_limit >= 17200 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY credit_limit_tier DESC

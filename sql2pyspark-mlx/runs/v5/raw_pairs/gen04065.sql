WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY credit_limit ASC, id ASC) AS row_num
    FROM customers
    QUALIFY row_num = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at < s.signup_date
        )
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM eligible
    EXCEPT ALL
    SELECT id
    FROM customers
    WHERE age > 54
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN remaining_keys k ON s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        REGEXP_EXTRACT(email, '@(.+)$', 1) AS email_domain,
        INITCAP(TRIM(city)) AS city_title
    FROM kept_rows
    WHERE name IS NOT NULL
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    segment,
    AVG(credit_limit) AS avg_credit_limit,
    STDDEV_SAMP(credit_limit) AS credit_limit_stddev,
    COUNT(DISTINCT segment) AS distinct_segments,
    COUNT_IF(country = 'SG') AS sg_events
FROM base
GROUP BY 1, 2
HAVING COUNT(DISTINCT segment) > 1

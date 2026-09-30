WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        HOUR(started_at) AS started_at_hour,
        LENGTH(country) AS country_length
    FROM sessions
    WHERE started_at > TO_DATE('2023-03-19')
      AND country = 'DE'
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM base s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.started_at >= sub.start_date
       AND s.started_at <= DATE_ADD(sub.end_date, 10)
),
matched_keys AS (
    -- Set difference on keys
    SELECT device, customer_id
    FROM matched
    EXCEPT ALL
    SELECT device, customer_id
    FROM sessions
    WHERE device NOT IN ('mobile', 'tablet')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM matched s
    INNER JOIN matched_keys k ON s.device = k.device AND s.customer_id = k.customer_id
),
kept_keys AS (
    -- Set difference on keys
    SELECT browser, customer_id
    FROM verified
    EXCEPT
    SELECT browser, customer_id
    FROM sessions
    WHERE customer_id = 10
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    INNER JOIN kept_keys k ON s.browser = k.browser AND s.customer_id = k.customer_id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.name,
        p.age,
        p.credit_limit
    FROM kept_rows a
    INNER JOIN customers p
        ON a.customer_id = p.id
),
enriched_sessions AS (
    -- Attach customers attributes
    SELECT e.*, b.country AS customer_country
    FROM enriched e
    LEFT JOIN customers b
        ON e.customer_id = b.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN credit_limit >= 13100 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_tier,
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    country,
    COUNT(DISTINCT country) AS distinct_countries,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN country = 'SG' THEN 1 END), 0), 2) AS monthly_fee_per_sg,
    MIN(subscription_id) AS min_subscription_id
FROM enriched_sessions
GROUP BY 1, 2, 3
ORDER BY min_subscription_id DESC;

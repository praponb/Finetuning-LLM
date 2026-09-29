WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS key, value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_TRUNC('year', created_at) created_at_period
    FROM exploded
    WHERE TO_DATE(created_at) < '2025-05-29'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        created_at,
        key,
        value,
        created_at_period,
        QUARTER(created_at) created_at_quarter,
        COALESCE(key, 'NONE') resolved_key
    FROM filtered
    WHERE created_at >= TO_DATE('2025-03-02')
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.key,
        src.created_at_period,
        src.created_at_quarter,
        src.resolved_key,
        c.segment
    FROM base src
    LEFT JOIN customers c
        ON src.customer_id = c.id
    WHERE c.country NOT IN ('SG', 'UK', 'TH')
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.signup_date,
        p.credit_limit,
        p.age
    FROM enriched a
    LEFT JOIN customers p
        ON a.customer_id <=> p.id
)
-- Multi-level aggregation
SELECT
    key,
    customer_id,
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    AVG(created_at_quarter) FILTER (WHERE key = 'gift') AS gift_avg_created_at_quarter,
    MAX(created_at_quarter) AS max_created_at_quarter,
    GROUPING_ID() AS gid
FROM with_dims
GROUP BY ROLLUP (key, customer_id, DATE_TRUNC('MONTH', signup_date))
ORDER BY key DESC

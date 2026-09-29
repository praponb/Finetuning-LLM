WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT device, country
    FROM sessions
    INTERSECT
    SELECT device, country
    FROM sessions
    WHERE customer_id IN (20, 38)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions s
    INNER JOIN remaining_keys k ON s.device = k.device AND s.country = k.country
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.device,
        src.browser,
        src.started_at,
        src.country,
        d.age
    FROM verified src
    INNER JOIN customers d
        ON src.customer_id = d.id
    WHERE d.age <> 53
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.device,
        s.browser,
        s.country,
        s.age,
        c.signup_date,
        c.city
    FROM joined s
    INNER JOIN customers c
        ON s.customer_id = c.id
),
joined_sessions AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.device,
        e.browser,
        e.country,
        e.age,
        e.signup_date,
        e.city,
        c.signup_date AS customer_signup_date,
        c.segment,
        c.name
    FROM enriched e
    LEFT JOIN customers c
        ON e.customer_id = c.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', customer_signup_date) AS activity_month,
    SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
    APPROX_PERCENTILE(age, 0.5) AS age_p50,
    COUNT(*) AS row_count
FROM joined_sessions
GROUP BY DATE_TRUNC('MONTH', customer_signup_date)
HAVING row_count < 1000
ORDER BY row_count DESC;

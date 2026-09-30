WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        element
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS element
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.created_at,
        b.name
    FROM exploded s
    INNER JOIN customers b
        ON s.customer_id <=> b.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(name)) AS name_title
    FROM with_dims
    WHERE customer_id IS NOT NULL
      AND LENGTH(name) < 13
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.name_title,
        d.email,
        d.city
    FROM filtered e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, SPLIT(LOWER(email), '@')[0] AS email_user
    FROM enriched
    WHERE city IN ('Chiang Mai', 'New York')
      AND city IS NOT NULL
)
-- Grouped metrics
SELECT
    customer_id,
    city,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    SIZE(COLLECT_SET(city)) AS num_cities,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE city = 'Bangkok') AS bangkok_rows,
    GROUPING(customer_id) AS grouping_id
FROM valid
GROUP BY CUBE (customer_id, city)
HAVING SIZE(COLLECT_SET(city)) > 1
ORDER BY customer_id NULLS FIRST;

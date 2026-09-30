WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        c.id,
        c.customer_id,
        item.product_id,
        item.price,
        item.promo
    FROM carts c
    LATERAL VIEW OUTER EXPLODE(c.items) t AS item
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CEIL(product_id) AS product_id_whole
    FROM exploded
    WHERE product_id != 7
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.price,
        p.age
    FROM valid e
    LEFT JOIN customers p
        ON e.customer_id <=> p.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.age,
        c.credit_limit,
        c.segment
    FROM joined a
    LEFT JOIN customers c
        ON a.customer_id = c.id
),
joined_carts AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.age,
        b.email,
        b.city,
        b.signup_date
    FROM with_dims s
    INNER JOIN customers b
        ON s.customer_id = b.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.credit_limit,
        c.name
    FROM joined_carts a
    LEFT JOIN customers c
        ON a.customer_id <=> c.id
),
joined_carts_2 AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.city,
        src.credit_limit,
        c.age AS customer_age,
        c.city AS customer_city
    FROM enriched src
    INNER JOIN customers c
        ON src.customer_id <=> c.id
    WHERE c.name LIKE '%Lee%'
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN customer_age >= 57 THEN 'PREMIUM' ELSE 'STANDARD' END AS customer_age_tier,
        APPROX_PERCENTILE(customer_age, 0.5) AS customer_age_p50,
        SORT_ARRAY(COLLECT_SET(customer_city)) AS customer_city_list,
        COUNT_IF(city = 'Bangkok') AS bangkok_events,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list_2,
        COUNT(*) AS event_count
    FROM joined_carts_2
    GROUP BY CASE WHEN customer_age >= 57 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    customer_age_tier,
    city_list_2,
    event_count
FROM daily
ORDER BY customer_age_tier DESC NULLS FIRST

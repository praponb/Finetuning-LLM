WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        qty,
        price,
        promo,
        EXPLODE_OUTER(coupon_codes) AS element
    FROM lines
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.country,
        b.credit_limit,
        b.city
    FROM exploded e
    INNER JOIN customers b
        ON e.customer_id = b.id
    WHERE b.credit_limit <= 9700
),
enriched_carts AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.price,
        s.country,
        s.credit_limit,
        p.city AS customer_city
    FROM enriched s
    LEFT JOIN customers p
        ON s.customer_id = p.id
)
-- Grouped metrics
SELECT
    CASE WHEN credit_limit >= 13400 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_tier,
    customer_id,
    MAX(price) FILTER (WHERE country = 'US') AS us_max_price,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(credit_limit, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_quartiles,
    COUNT(*) FILTER (WHERE customer_city = 'Berlin') AS berlin_rows,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    GROUPING_ID() AS gid
FROM enriched_carts
GROUP BY ROLLUP (CASE WHEN credit_limit >= 13400 THEN 'HIGH' ELSE 'LOW' END, customer_id)
HAVING COUNT(*) > 1

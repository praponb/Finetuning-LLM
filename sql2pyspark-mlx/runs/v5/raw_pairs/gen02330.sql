WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        t.id,
        t.coupon_codes,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts t
    LATERAL VIEW EXPLODE(t.items) e AS elem
),
exploded_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        price,
        promo,
        EXPLODE(coupon_codes) AS element
    FROM exploded
),
enriched AS (
    -- Ranking and gap features
    SELECT
        price,
        promo,
        MIN(price) OVER (PARTITION BY promo) promo_min_price,
        CUME_DIST() OVER (PARTITION BY element ORDER BY price) price_cume_dist
    FROM exploded_carts
),
scored AS (
    -- Running and moving aggregates
    SELECT
        price,
        price_cume_dist,
        CUME_DIST() OVER (ORDER BY price_cume_dist DESC NULLS LAST) price_cume_dist_cume_dist,
        CUME_DIST() OVER (ORDER BY price_cume_dist) price_cume_dist_cume_dist_2
    FROM enriched
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN price_cume_dist_cume_dist_2 >= 0.81 THEN 'HIGH' ELSE 'LOW' END AS price_cume_dist_cume_dist_2_tier,
        CASE WHEN price_cume_dist_cume_dist >= 0.61 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_cume_dist_cume_dist_tier,
        ROUND(AVG(price_cume_dist_cume_dist_2), 2) AS avg_price_cume_dist_cume_dist_2,
        COUNT(*) AS event_count,
        PERCENTILE_APPROX(price_cume_dist, 0.5) AS price_cume_dist_p50,
        GROUPING(CASE WHEN price_cume_dist_cume_dist_2 >= 0.81 THEN 'HIGH' ELSE 'LOW' END) AS grouping_level
    FROM scored
    GROUP BY GROUPING SETS ((CASE WHEN price_cume_dist_cume_dist_2 >= 0.81 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN price_cume_dist_cume_dist >= 0.61 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN price_cume_dist_cume_dist_2 >= 0.81 THEN 'HIGH' ELSE 'LOW' END), ())
)
SELECT
    price_cume_dist_cume_dist_2_tier,
    price_cume_dist_cume_dist_tier,
    avg_price_cume_dist_cume_dist_2,
    event_count,
    price_cume_dist_p50,
    grouping_level
FROM daily

WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        c.id,
        c.created_at,
        c.coupon_codes,
        pos + 1 AS item_no,
        rec.qty,
        rec.price
    FROM carts c
    LATERAL VIEW OUTER POSEXPLODE(c.items) x AS pos, rec
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        created_at,
        price,
        EXPLODE(coupon_codes) AS value
    FROM flattened
),
scored AS (
    -- Running and moving aggregates
    SELECT
        created_at,
        price,
        ROUND(price / SUM(price) OVER (PARTITION BY value), 4) AS price_share,
        RANK() OVER (ORDER BY price DESC) AS price_rank
    FROM lines
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        price,
        price_share,
        price_rank,
        PERCENT_RANK() OVER w_recent AS price_rank_percent_rank,
        CUME_DIST() OVER (ORDER BY price DESC NULLS LAST) AS price_cume_dist,
        MAX(price) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS price_max_1h
    FROM scored
    WHERE price_rank BETWEEN 4 AND 8
    WINDOW w_recent AS (ORDER BY price_rank DESC NULLS FIRST)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        price_share,
        price_cume_dist,
        RANK() OVER (ORDER BY price DESC) AS price_rank_2,
        DENSE_RANK() OVER (ORDER BY price NULLS LAST) AS price_dense_rank
    FROM metrics
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN price_cume_dist >= 0.9 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_cume_dist_tier,
    ROUND(AVG(price_dense_rank), 2) AS avg_price_dense_rank,
    PERCENTILE_APPROX(price_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS price_dense_rank_quartiles,
    ROUND(AVG(price_dense_rank), 2) AS avg_price_dense_rank_2,
    PERCENTILE_APPROX(price_rank_2, ARRAY(0.25, 0.5, 0.75)) AS price_rank_2_quartiles
FROM with_windows
GROUP BY CASE WHEN price_cume_dist >= 0.9 THEN 'PREMIUM' ELSE 'STANDARD' END;

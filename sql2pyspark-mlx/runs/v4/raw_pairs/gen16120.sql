WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        pos,
        line.product_id,
        line.qty,
        line.promo
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) t AS pos, line
),
flattened_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        pos,
        product_id,
        qty,
        EXPLODE(coupon_codes) AS code
    FROM flattened
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        created_at,
        pos,
        product_id,
        qty,
        code,
        DENSE_RANK() OVER (ORDER BY pos DESC NULLS FIRST) AS pos_dense_rank,
        MAX(qty) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS qty_max_30m
    FROM flattened_carts
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.city,
        d.signup_date
    FROM with_windows src
    INNER JOIN customers d
        ON src.customer_id = d.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        created_at,
        pos,
        product_id,
        qty,
        code,
        pos_dense_rank,
        qty_max_30m,
        city,
        signup_date,
        COUNT(qty_max_30m) OVER (PARTITION BY customer_id, city ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS qty_max_30m_count_30m,
        ROUND(qty_max_30m / SUM(qty_max_30m) OVER (PARTITION BY customer_id), 4) AS qty_max_30m_share
    FROM with_dims
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        created_at,
        pos,
        product_id,
        qty,
        code,
        pos_dense_rank,
        qty_max_30m,
        city,
        signup_date,
        qty_max_30m_count_30m,
        qty_max_30m_share,
        STDDEV_SAMP(qty) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_qty,
        MAX(pos) OVER (PARTITION BY code ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS pos_max_30m,
        AVG(qty_max_30m_count_30m) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS qty_max_30m_count_30m_avg_15m
    FROM metrics
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN pos >= 3 THEN 'PREMIUM' ELSE 'STANDARD' END AS pos_tier,
    CASE WHEN qty_max_30m_count_30m >= 16 THEN 'HIGH' ELSE 'LOW' END AS qty_max_30m_count_30m_tier,
    city,
    COUNT(DISTINCT city) AS distinct_cities,
    MAX(qty_max_30m) FILTER (WHERE code = 'SPRING10') AS spring10_max_qty_max_30m,
    SUM(product_id) AS total_product_id
FROM enriched
GROUP BY GROUPING SETS ((CASE WHEN pos >= 3 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN qty_max_30m_count_30m >= 16 THEN 'HIGH' ELSE 'LOW' END, city), (CASE WHEN pos >= 3 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN qty_max_30m_count_30m >= 16 THEN 'HIGH' ELSE 'LOW' END))
HAVING distinct_cities > 1;

WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        item.product_id,
        item.qty,
        item.price,
        item.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) t AS item
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LEAST(price, product_id) AS least_price_product_id
    FROM flattened
    WHERE created_at >= TIMESTAMP '2025-03-02 12:00:00'
      AND product_id >= 5
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        qty,
        price,
        promo,
        CASE WHEN promo IN ('SPRING10', 'BUNDLE') THEN 'Tier 1' ELSE 'Other' END AS promo_group,
        COALESCE(promo, 'Unknown') AS promo_filled
    FROM cleaned
    WHERE created_at > TO_DATE('2025-03-01')
      AND promo IS NOT NULL
      AND price BETWEEN 60 AND 200
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(product_id) OVER (PARTITION BY promo) AS promo_sum_product_id,
        ROUND(qty / SUM(qty) OVER (PARTITION BY promo), 4) AS qty_share
    FROM valid
)
-- Multi-level aggregation
SELECT
    CASE WHEN qty >= 5 THEN 'LARGE' ELSE 'SMALL' END AS qty_tier,
    promo,
    COUNT(CASE WHEN promo = 'SPRING10' THEN 1 END) AS spring10_count,
    SORT_ARRAY(COLLECT_SET(promo)) AS promo_list,
    COUNT(CASE WHEN promo = 'BUNDLE' THEN 1 END) AS bundle_count,
    SIZE(COLLECT_SET(promo)) AS num_promos,
    APPROX_PERCENTILE(product_id, ARRAY(0.25, 0.5, 0.75)) AS product_id_quartiles,
    COUNT(CASE WHEN promo = 'WELCOME5' THEN 1 END) AS welcome5_count,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN qty >= 5 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(CASE WHEN promo = 'SPRING10' THEN 1 END) DESC) AS spring10_count_dense_rank
FROM metrics
GROUP BY CASE WHEN qty >= 5 THEN 'LARGE' ELSE 'SMALL' END, promo
ORDER BY welcome5_count DESC;

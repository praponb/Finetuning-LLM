WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        item.product_id,
        item.qty,
        item.price,
        item.promo
    FROM (
        SELECT
            id,
            customer_id,
            coupon_codes,
            EXPLODE(items) AS item
        FROM carts
    ) li
),
lines_carts AS (
    -- Unnest the array into one row per element
    SELECT
        qty,
        price,
        promo,
        EXPLODE(coupon_codes) AS element
    FROM lines
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        qty,
        price,
        promo,
        DENSE_RANK() OVER (PARTITION BY element ORDER BY price NULLS FIRST) AS price_dense_rank,
        RANK() OVER (PARTITION BY promo ORDER BY price DESC NULLS LAST) AS price_rank,
        ROUND(price / SUM(price) OVER (PARTITION BY element), 4) AS price_share,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY price DESC NULLS LAST) AS price_percent_rank
    FROM lines_carts
    WHERE price IS NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        GREATEST(price, price_share) AS greatest_price_price_share,
        qty + price AS qty_plus_price
    FROM scored
    WHERE price <= 200
      AND promo IS DISTINCT FROM 'BUNDLE'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        price_rank - price_share AS price_rank_minus_price_share,
        COALESCE(promo, 'N/A') AS promo_filled
    FROM filtered
    WHERE promo IS NOT DISTINCT FROM 'BUNDLE'
)
-- Grouped metrics
SELECT
    CASE WHEN price >= 263 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_tier,
    COUNT(*) AS num_rows,
    COUNT(*) FILTER (WHERE promo = 'BUNDLE') AS bundle_rows,
    SORT_ARRAY(COLLECT_SET(promo)) AS promo_list,
    ROUND(SUM(price_share) / NULLIF(COUNT(CASE WHEN promo = 'BUNDLE' THEN 1 END), 0), 2) AS price_share_per_bundle
FROM base
GROUP BY 1;

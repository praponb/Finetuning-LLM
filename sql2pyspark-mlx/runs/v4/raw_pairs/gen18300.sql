WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        pos,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) li AS pos, elem
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        pos,
        product_id,
        qty,
        price,
        EXPLODE(coupon_codes) AS tag
    FROM flattened
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        product_id * price AS product_id_times_price,
        CASE WHEN product_id >= 10 THEN 'High' WHEN product_id >= 3 THEN 'Medium' ELSE 'Low' END AS product_id_level
    FROM lines
    WHERE product_id = 4
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        pos,
        product_id,
        qty,
        price,
        tag,
        product_id_times_price,
        product_id_level,
        MAX(product_id) OVER (PARTITION BY tag) AS tag_max_product_id,
        ROUND(qty / SUM(qty) OVER (PARTITION BY tag), 4) AS qty_share,
        PERCENT_RANK() OVER (PARTITION BY tag ORDER BY product_id DESC NULLS LAST) AS product_id_percent_rank
    FROM base
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY product_id_times_price) AS product_id_times_price_rank,
        PERCENT_RANK() OVER (PARTITION BY tag ORDER BY product_id_percent_rank DESC NULLS FIRST) AS product_id_percent_rank_percent_rank,
        CUME_DIST() OVER (PARTITION BY tag ORDER BY pos NULLS FIRST) AS pos_cume_dist
    FROM metrics
)
SELECT
    product_id,
    price,
    tag,
    product_id_times_price,
    product_id_level,
    tag_max_product_id,
    qty_share,
    product_id_percent_rank,
    product_id_percent_rank_percent_rank
FROM scored
ORDER BY product_id_times_price NULLS LAST

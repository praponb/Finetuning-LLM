WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        line.product_id,
        line.qty,
        line.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) li AS line
),
exploded_carts AS (
    -- Unnest the array into one row per element
    SELECT
        customer_id,
        created_at,
        product_id,
        promo,
        EXPLODE(coupon_codes) AS tag
    FROM exploded
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        customer_id,
        created_at,
        product_id,
        promo,
        tag,
        PERCENT_RANK() OVER (PARTITION BY promo, customer_id ORDER BY product_id DESC NULLS FIRST) AS product_id_percent_rank,
        AVG(product_id) OVER (PARTITION BY customer_id, promo) AS customer_id_avg_product_id
    FROM exploded_carts
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.age,
        p.city,
        p.segment
    FROM with_windows a
    INNER JOIN customers p
        ON a.customer_id = p.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        customer_id,
        created_at,
        product_id,
        promo,
        tag,
        customer_id_avg_product_id,
        city,
        segment,
        FLOOR(customer_id_avg_product_id) AS customer_id_avg_product_id_whole,
        CASE WHEN promo IN ('VIP', 'SPRING10') THEN 'Group A' ELSE 'Other' END AS promo_group
    FROM joined
    WHERE segment IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        promo,
        customer_id_avg_product_id
    FROM base
) src
PIVOT (
    AVG(customer_id_avg_product_id) AS amount,
    COUNT(1) AS cnt
    FOR promo IN ('BUNDLE' AS bundle, 'SPRING10' AS spring10)
);

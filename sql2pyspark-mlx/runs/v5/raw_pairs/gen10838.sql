WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        INLINE(items)
    FROM carts
),
exploded_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        product_id,
        qty,
        EXPLODE_OUTER(coupon_codes) AS code
    FROM exploded
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        qty,
        code,
        DENSE_RANK() OVER (ORDER BY product_id DESC NULLS LAST) AS product_id_dense_rank,
        RANK() OVER (PARTITION BY code ORDER BY qty DESC) AS qty_rank,
        DENSE_RANK() OVER (PARTITION BY code ORDER BY qty) AS qty_dense_rank
    FROM exploded_carts
    WHERE qty IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        qty,
        code,
        product_id_dense_rank,
        RANK() OVER (PARTITION BY code ORDER BY qty_rank DESC NULLS LAST) AS qty_rank_rank
    FROM with_windows
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        qty - qty_rank_rank AS qty_minus_qty_rank_rank,
        COALESCE(qty_rank_rank, 0) AS qty_rank_rank_filled
    FROM scored
    WHERE code IS DISTINCT FROM 'BUNDLE'
)
SELECT
    id,
    qty,
    qty_rank_rank,
    qty_minus_qty_rank_rank,
    qty_rank_rank_filled
FROM valid

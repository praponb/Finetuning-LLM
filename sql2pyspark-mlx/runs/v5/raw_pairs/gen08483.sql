WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        item.product_id,
        item.qty,
        item.promo
    FROM (
        SELECT
            id,
            customer_id,
            coupon_codes,
            EXPLODE(items) AS item
        FROM carts
    ) x
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.segment,
        b.email,
        b.city
    FROM lines s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        qty,
        promo,
        segment,
        email,
        city,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY product_id) product_id_cume_dist,
        DENSE_RANK() OVER (PARTITION BY segment ORDER BY qty NULLS LAST) qty_dense_rank,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY city), 4) product_id_share,
        RANK() OVER (PARTITION BY segment ORDER BY qty NULLS FIRST) qty_rank,
        RANK() OVER (PARTITION BY city ORDER BY qty NULLS LAST) qty_rank_2
    FROM enriched
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        qty,
        email,
        city,
        product_id_cume_dist,
        qty_dense_rank,
        product_id_share,
        qty_rank_2,
        ROUND(qty / SUM(qty) OVER (PARTITION BY promo), 4) qty_share,
        ROUND(qty_dense_rank / SUM(qty_dense_rank) OVER (PARTITION BY segment), 4) qty_dense_rank_share,
        MAX(qty_rank_2) OVER (PARTITION BY promo) promo_max_qty_rank_2
    FROM metrics
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        city,
        product_id_cume_dist,
        qty_dense_rank,
        qty_rank_2,
        qty_share,
        qty_dense_rank_share,
        qty_share + qty_rank_2 qty_share_plus_qty_rank_2
    FROM with_windows
    WHERE city IS DISTINCT FROM 'Singapore'
),
with_windows_carts AS (
    -- Running and moving aggregates
    SELECT *, ROUND(qty_dense_rank / SUM(qty_dense_rank) OVER (PARTITION BY city), 4) qty_dense_rank_share_2
    FROM base
)
SELECT
    id,
    qty_rank_2,
    qty_share,
    qty_share_plus_qty_rank_2,
    qty_dense_rank_share_2
FROM with_windows_carts
ORDER BY qty_rank_2 DESC;

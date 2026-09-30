WITH flattened AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        item.product_id,
        item.qty,
        item.price
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) x AS item
),
cleaned AS (
    SELECT
        created_at,
        coupon_codes,
        product_id,
        price,
        ARRAY_JOIN(coupon_codes, '|') AS coupon_codes_text,
        GREATEST(price, qty) AS greatest_price_qty
    FROM flattened
    WHERE created_at >= DATE '2025-03-01'
),
enriched AS (
    SELECT
        *,
        DENSE_RANK() OVER w_grp AS greatest_price_qty_dense_rank,
        COUNT(greatest_price_qty) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS greatest_price_qty_count_2h
    FROM cleaned
    WHERE greatest_price_qty < 47
    WINDOW w_grp AS (ORDER BY greatest_price_qty DESC NULLS FIRST)
),
metrics AS (
    SELECT
        created_at,
        product_id,
        price,
        coupon_codes_text,
        greatest_price_qty,
        greatest_price_qty_count_2h,
        MAX(greatest_price_qty_count_2h) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS greatest_price_qty_count_2h_max_30m,
        CUME_DIST() OVER (ORDER BY greatest_price_qty_count_2h DESC NULLS FIRST) AS greatest_price_qty_count_2h_cume_dist,
        COUNT(greatest_price_qty_dense_rank) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS greatest_price_qty_dense_rank_count_2h
    FROM enriched
),
enriched_carts AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY greatest_price_qty NULLS FIRST) AS greatest_price_qty_cume_dist,
        CUME_DIST() OVER (ORDER BY greatest_price_qty_count_2h_max_30m) AS greatest_price_qty_count_2h_max_30m_cume_dist,
        RANK() OVER (ORDER BY greatest_price_qty_count_2h_max_30m DESC NULLS FIRST) AS greatest_price_qty_count_2h_max_30m_rank,
        MAX(greatest_price_qty_count_2h_max_30m) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS greatest_price_qty_count_2h_max_30m_max_30m
    FROM metrics
)
SELECT
    product_id,
    AVG(product_id) AS avg_product_id,
    MAX(created_at) AS last_created_at,
    COUNT(*) AS num_rows,
    SUM(greatest_price_qty_cume_dist) AS total_greatest_price_qty_cume_dist
FROM enriched_carts
GROUP BY product_id
ORDER BY num_rows DESC

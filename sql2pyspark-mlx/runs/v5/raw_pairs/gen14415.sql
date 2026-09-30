WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        pos + 1 AS line_no,
        item.product_id,
        item.qty,
        item.promo
    FROM carts
    LATERAL VIEW POSEXPLODE(items) e AS pos, item
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY customer_id), 4) AS product_id_share,
        ROW_NUMBER() OVER (ORDER BY created_at DESC, id, line_no) AS row_seq,
        MAX(qty) OVER (PARTITION BY customer_id) AS customer_id_max_qty,
        SUM(product_id) OVER (PARTITION BY customer_id, promo ORDER BY product_id, id, line_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_product_id
    FROM exploded
    WHERE qty < 3
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(product_id_share AS INT) AS product_id_share_int,
        SIZE(coupon_codes) AS coupon_codes_count,
        COALESCE(promo, 'DIRECT') AS resolved_promo
    FROM scored
    WHERE line_no > 3
),
scored_carts AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(row_seq) OVER (PARTITION BY customer_id, promo ORDER BY customer_id_max_qty, id, line_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq,
        LAST_VALUE(promo) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY product_id, id, line_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_promo,
        LAG(customer_id_max_qty, 2, 0) OVER (ORDER BY coupon_codes_count, id, line_no) AS prev_customer_id_max_qty
    FROM cleaned
)
SELECT
    id,
    coupon_codes,
    line_no,
    product_id,
    promo,
    product_id_share,
    customer_id_max_qty,
    running_product_id,
    product_id_share_int,
    CASE WHEN promo RLIKE '^(WELCOME5|SPRING10)$' THEN 'CORE' WHEN promo RLIKE '^(VIP|BUNDLE)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS promo_zone
FROM scored_carts
ORDER BY promo DESC NULLS FIRST

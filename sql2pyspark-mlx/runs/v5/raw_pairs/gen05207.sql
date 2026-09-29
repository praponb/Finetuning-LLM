WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        c.id,
        c.customer_id,
        c.created_at,
        c.coupon_codes,
        pos + 1 AS position,
        li_item.product_id,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM carts c
    LATERAL VIEW OUTER POSEXPLODE(c.items) e AS pos, li_item
)
-- Window metrics over several different partitions
SELECT
    id,
    customer_id,
    coupon_codes,
    product_id,
    qty,
    price,
    CUME_DIST() OVER (PARTITION BY customer_id ORDER BY product_id DESC NULLS FIRST) AS product_id_cume_dist,
    SUM(product_id) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS product_id_sum_1h
FROM lines

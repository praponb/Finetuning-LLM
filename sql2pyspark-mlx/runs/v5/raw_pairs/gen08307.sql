WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        c.id,
        c.customer_id,
        c.created_at,
        c.coupon_codes,
        line.qty,
        line.promo
    FROM carts c
    LATERAL VIEW EXPLODE(c.items) e AS line
)
-- Window metrics over several different partitions
SELECT
    id,
    created_at,
    promo,
    SUM(qty) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS qty_sum_15m,
    MAX(qty) OVER (PARTITION BY promo) AS promo_max_qty
FROM lines;

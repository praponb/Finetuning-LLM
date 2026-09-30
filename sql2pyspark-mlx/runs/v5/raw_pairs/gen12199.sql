WITH flattened AS (
    SELECT
        c.id,
        c.created_at,
        c.coupon_codes,
        pos + 1 AS position,
        rec.product_id,
        rec.qty,
        rec.price,
        rec.promo
    FROM carts c
    LATERAL VIEW OUTER POSEXPLODE(c.items) x AS pos, rec
),
filtered AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        product_id,
        qty,
        price,
        promo,
        qty - product_id AS qty_minus_product_id,
        ROUND(price * 1.15, 2) AS price_scaled
    FROM flattened
    WHERE created_at >= TO_DATE('2025-03-01')
      AND promo IS NOT NULL
),
enriched AS (
    SELECT
        *,
        SUM(product_id) OVER (PARTITION BY promo) AS promo_sum_product_id,
        MAX(qty) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS qty_max_2h,
        MAX(qty_minus_product_id) OVER (PARTITION BY promo) AS promo_max_qty_minus_product_id
    FROM filtered
),
with_windows AS (
    SELECT
        created_at,
        coupon_codes,
        product_id,
        qty,
        price,
        qty_minus_product_id,
        promo_sum_product_id,
        qty_max_2h,
        promo_max_qty_minus_product_id,
        COUNT(price_scaled) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS price_scaled_count_15m,
        STDDEV_SAMP(qty) OVER (PARTITION BY promo) AS promo_stddev_samp_qty,
        AVG(qty) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS qty_avg_2h,
        MAX(price) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS price_max_30m
    FROM enriched
    WHERE promo_max_qty_minus_product_id <= 62
),
cleaned AS (
    SELECT
        coupon_codes,
        qty,
        price,
        promo_sum_product_id,
        qty_max_2h,
        promo_max_qty_minus_product_id,
        price_scaled_count_15m,
        promo_stddev_samp_qty,
        qty_avg_2h,
        price_max_30m,
        qty * promo_max_qty_minus_product_id AS qty_times_promo_max_qty_minus_product_id
    FROM with_windows
    WHERE created_at > TIMESTAMP '2025-03-01 20:00:00'
      AND product_id <= 7
)
SELECT
    qty,
    price,
    promo_max_qty_minus_product_id,
    price_scaled_count_15m,
    promo_stddev_samp_qty,
    qty_avg_2h
FROM cleaned
ORDER BY price;

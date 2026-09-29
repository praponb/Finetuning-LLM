WITH lines AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        INLINE(items)
    FROM carts
),
flattened AS (
    SELECT
        customer_id,
        qty,
        promo,
        EXPLODE(coupon_codes) AS code
    FROM lines
),
with_windows AS (
    SELECT
        promo,
        code,
        MIN(qty) OVER (PARTITION BY customer_id, code) AS customer_id_min_qty,
        MAX(qty) OVER (PARTITION BY code) AS code_max_qty,
        ROUND(qty / SUM(qty) OVER (PARTITION BY customer_id), 4) AS qty_share
    FROM flattened
),
base AS (
    SELECT
        *,
        CASE WHEN code_max_qty >= 5 THEN 'High' WHEN code_max_qty >= 2 THEN 'Medium' ELSE 'Low' END AS code_max_qty_tier,
        COALESCE(promo, 'OTHER') AS promo_clean
    FROM with_windows
    WHERE code IS NOT NULL
      AND customer_id_min_qty <= 3
),
scored AS (
    SELECT
        promo,
        code,
        code_max_qty,
        code_max_qty_tier,
        promo_clean,
        PERCENT_RANK() OVER (ORDER BY customer_id_min_qty DESC NULLS LAST) AS customer_id_min_qty_percent_rank,
        MAX(customer_id_min_qty) OVER (PARTITION BY promo) AS promo_max_customer_id_min_qty,
        RANK() OVER (ORDER BY qty_share) AS qty_share_rank
    FROM base
),
daily AS (
    SELECT
        CASE WHEN promo_max_customer_id_min_qty >= 4 THEN 'LARGE' ELSE 'SMALL' END AS promo_max_customer_id_min_qty_tier,
        SUM(CASE WHEN code = 'VIP' THEN promo_max_customer_id_min_qty ELSE 0 END) AS vip_promo_max_customer_id_min_qty,
        COUNT(*) AS cnt,
        COUNT(*) AS event_count
    FROM scored
    GROUP BY CASE WHEN promo_max_customer_id_min_qty >= 4 THEN 'LARGE' ELSE 'SMALL' END
    HAVING cnt > 1
)
SELECT
    promo_max_customer_id_min_qty_tier,
    cnt,
    event_count
FROM daily
ORDER BY promo_max_customer_id_min_qty_tier NULLS FIRST;

WITH lines AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
filtered AS (
    SELECT
        id,
        coupon_codes,
        qty,
        price,
        promo,
        LOWER(promo) AS promo_lower,
        DATE_FORMAT(created_at, 'yyyy-MM') AS created_at_label
    FROM lines
    WHERE created_at >= TIMESTAMP '2025-03-01 11:00:00'
),
base AS (
    SELECT
        id,
        coupon_codes,
        price,
        promo,
        promo_lower,
        ARRAY_CONTAINS(coupon_codes, 'VIP') AS is_vip,
        COALESCE(promo, 'DIRECT') AS resolved_promo
    FROM filtered
    WHERE qty < 4
),
cleaned AS (
    SELECT
        *,
        CAST(is_vip AS INT) AS is_vip_flag,
        CAST(is_vip AS INT) AS is_vip_flag_2
    FROM base
    WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
),
enriched AS (
    SELECT
        *,
        RANK() OVER w_hist AS is_vip_flag_2_rank,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY is_vip_flag DESC NULLS FIRST) AS is_vip_flag_cume_dist,
        CUME_DIST() OVER w_part AS is_vip_flag_2_cume_dist,
        RANK() OVER w_part_2 AS price_rank
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY promo ORDER BY is_vip_flag_2 NULLS LAST), w_part AS (ORDER BY is_vip_flag_2), w_part_2 AS (PARTITION BY promo ORDER BY price DESC NULLS FIRST)
)
SELECT
    id,
    coupon_codes,
    price,
    promo,
    promo_lower,
    resolved_promo,
    is_vip_flag,
    is_vip_flag_2_rank,
    price_rank,
    CASE WHEN promo RLIKE '^(BUNDLE|SPRING10)$' THEN 'GROUP_B' WHEN promo RLIKE '^(VIP|WELCOME5)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS promo_zone
FROM enriched
ORDER BY is_vip_flag NULLS LAST;

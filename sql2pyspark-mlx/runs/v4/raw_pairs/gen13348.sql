WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) t AS li_item
),
flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        qty,
        price,
        promo,
        element
    FROM exploded
    LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS element
),
enriched AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY promo ORDER BY price NULLS LAST) AS price_rank,
        RANK() OVER (ORDER BY qty) AS qty_rank,
        COUNT(qty) OVER (PARTITION BY element) AS element_count_qty,
        RANK() OVER (ORDER BY price DESC NULLS LAST) AS price_rank_2
    FROM flattened
),
cleaned AS (
    SELECT
        id,
        customer_id,
        created_at,
        qty,
        price,
        promo,
        element,
        price_rank,
        qty_rank,
        element_count_qty,
        price_rank_2,
        ROUND(price_rank, 2) AS price_rank_rounded
    FROM enriched
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 17:00:00')
      AND customer_id IS NOT NULL
      AND price IS NOT NULL
),
base AS (
    SELECT
        *,
        MONTH(created_at) AS created_at_month,
        COALESCE(element, 'UNKNOWN') AS element_filled
    FROM cleaned
    WHERE created_at > TO_DATE('2025-03-01')
      AND element IS NOT NULL
      AND price_rank_2 IS NULL
)
SELECT
    id,
    created_at,
    qty,
    promo,
    price_rank,
    qty_rank,
    element_count_qty,
    price_rank_2,
    price_rank_rounded,
    CASE WHEN promo RLIKE '^(WELCOME5|SPRING10)$' THEN 'PRIORITY' WHEN promo RLIKE '^(VIP|BUNDLE)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS promo_zone
FROM base
ORDER BY price_rank_2 DESC;

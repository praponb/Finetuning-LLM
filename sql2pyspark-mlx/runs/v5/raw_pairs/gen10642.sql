WITH exploded AS (
    SELECT
        base.id,
        base.customer_id,
        base.coupon_codes,
        li_item.qty,
        li_item.price
    FROM carts base
    LATERAL VIEW EXPLODE(base.items) li AS li_item
),
filtered AS (
    SELECT
        customer_id,
        coupon_codes,
        qty,
        ARRAY_CONTAINS(coupon_codes, 'WELCOME5') AS is_welcome5
    FROM exploded
    WHERE customer_id IS NOT NULL
      AND price BETWEEN 60 AND 130
),
matched_keys AS (
    SELECT customer_id
    FROM filtered
    INTERSECT
    SELECT customer_id
    FROM carts
    WHERE SIZE(coupon_codes) > 1
),
reconciled AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id
),
enriched AS (
    SELECT src.*, d.email
    FROM reconciled src
    LEFT JOIN customers d
        ON src.customer_id <=> d.id
),
metrics AS (
    SELECT
        *,
        ROUND(qty / SUM(qty) OVER (PARTITION BY customer_id), 4) AS qty_share,
        RANK() OVER (ORDER BY qty DESC) AS qty_rank
    FROM enriched
),
valid AS (
    SELECT
        customer_id,
        coupon_codes,
        qty,
        is_welcome5,
        email,
        qty_rank,
        ROUND(qty_share, 1) AS qty_share_rounded,
        SIZE(coupon_codes) AS coupon_codes_count
    FROM metrics
    WHERE qty >= 2
),
daily AS (
    SELECT
        CASE WHEN coupon_codes_count >= 79 THEN 'PREMIUM' ELSE 'STANDARD' END AS coupon_codes_count_tier,
        CASE WHEN qty_share_rounded >= 65 THEN 'LARGE' ELSE 'SMALL' END AS qty_share_rounded_tier,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids,
        COUNT(*) AS num_rows,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids_2,
        MAX(coupon_codes_count) AS max_coupon_codes_count,
        COUNT(*) AS event_count
    FROM valid
    GROUP BY coupon_codes_count_tier, qty_share_rounded_tier
)
SELECT
    coupon_codes_count_tier,
    qty_share_rounded_tier,
    num_rows,
    distinct_customer_ids_2,
    max_coupon_codes_count,
    event_count
FROM daily
ORDER BY qty_share_rounded_tier DESC NULLS FIRST

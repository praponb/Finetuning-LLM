WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        INLINE(items)
    FROM carts
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, SIZE(coupon_codes) AS coupon_codes_count
    FROM lines
    WHERE promo IN ('VIP', 'BUNDLE', 'WELCOME5')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        price,
        coupon_codes_count,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) AS product_id_share,
        PERCENT_RANK() OVER (ORDER BY price DESC) AS price_percent_rank,
        PERCENT_RANK() OVER w_part AS coupon_codes_count_percent_rank,
        AVG(qty) OVER (PARTITION BY promo) AS promo_avg_qty
    FROM filtered
    WINDOW w_part AS (PARTITION BY promo ORDER BY coupon_codes_count NULLS FIRST)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM scored
    EXCEPT ALL
    SELECT id
    FROM carts
    WHERE HOUR(created_at) BETWEEN 6 AND 13
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.id = k.id
),
filtered_carts AS (
    -- Basic cleansing and defaults
    SELECT
        product_id_share,
        coupon_codes_count_percent_rank,
        promo_avg_qty,
        GREATEST(coupon_codes_count_percent_rank, coupon_codes_count) AS greatest_coupon_codes_count_percent_rank_coupon_codes_count
    FROM reconciled
    WHERE price_percent_rank >= 0.49
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        coupon_codes_count_percent_rank - greatest_coupon_codes_count_percent_rank_coupon_codes_count AS coupon_codes_count_percent_rank_minus_greatest_coupon_codes_count_percent_rank_coupon_codes_count,
        greatest_coupon_codes_count_percent_rank_coupon_codes_count * coupon_codes_count_percent_rank AS greatest_coupon_codes_count_percent_rank_coupon_codes_count_times_coupon_codes_count_percent_rank
    FROM filtered_carts
    WHERE product_id_share >= 0.6
),
scored_carts AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY promo_avg_qty) AS promo_avg_qty_percent_rank,
        RANK() OVER (ORDER BY product_id_share DESC NULLS LAST) AS product_id_share_rank
    FROM cleaned
)
SELECT
    product_id_share,
    coupon_codes_count_percent_rank,
    promo_avg_qty,
    greatest_coupon_codes_count_percent_rank_coupon_codes_count,
    greatest_coupon_codes_count_percent_rank_coupon_codes_count_times_coupon_codes_count_percent_rank,
    promo_avg_qty_percent_rank,
    product_id_share_rank
FROM scored_carts
ORDER BY product_id_share_rank NULLS FIRST

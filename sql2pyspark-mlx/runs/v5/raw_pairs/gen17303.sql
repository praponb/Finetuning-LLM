WITH lines AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price
    FROM (
        SELECT
            id,
            created_at,
            coupon_codes,
            EXPLODE(items) AS line
        FROM carts
    ) x
),
enriched AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY price DESC) AS price_percent_rank,
        PERCENT_RANK() OVER (ORDER BY price NULLS LAST) AS price_percent_rank_2,
        RANK() OVER w_running AS qty_rank
    FROM lines
    WINDOW w_running AS (ORDER BY qty DESC NULLS LAST)
),
cleaned AS (
    SELECT
        *,
        price_percent_rank - qty_rank AS price_percent_rank_minus_qty_rank,
        CAST(qty AS DOUBLE) AS qty_str
    FROM enriched
    WHERE created_at > TO_DATE('2025-03-01')
      AND product_id > 4
),
scored AS (
    SELECT
        *,
        AVG(price_percent_rank_2) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS price_percent_rank_2_avg_1h,
        RANK() OVER w_ordered AS qty_rank_rank
    FROM cleaned
    WHERE qty_str BETWEEN 21 AND 45
    WINDOW w_ordered AS (ORDER BY qty_rank DESC NULLS FIRST)
),
cleaned_carts AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        price,
        price_percent_rank,
        price_percent_rank_2,
        qty_rank,
        price_percent_rank_2_avg_1h,
        qty_rank_rank,
        price_percent_rank_2_avg_1h * qty AS price_percent_rank_2_avg_1h_times_qty
    FROM scored
    WHERE created_at > DATE '2025-03-02'
),
valid AS (
    SELECT
        *,
        ROUND(qty_rank, 0) AS qty_rank_rounded,
        CAST(qty_rank_rank AS DOUBLE) AS qty_rank_rank_str
    FROM cleaned_carts
    WHERE created_at > TO_DATE('2025-03-01')
      AND ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
)
SELECT
    CASE WHEN price_percent_rank_2_avg_1h_times_qty >= 86 THEN 'HIGH' ELSE 'LOW' END AS price_percent_rank_2_avg_1h_times_qty_tier,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    MAX(created_at) AS ended_at,
    MAX(created_at) AS last_seen,
    COUNT(*) AS row_count
FROM valid
GROUP BY CASE WHEN price_percent_rank_2_avg_1h_times_qty >= 86 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('DAY', created_at)

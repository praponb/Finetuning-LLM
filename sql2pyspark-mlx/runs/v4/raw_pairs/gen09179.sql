WITH lines AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
lines_carts AS (
    SELECT
        created_at,
        product_id,
        promo,
        EXPLODE(coupon_codes) AS element
    FROM lines
),
base AS (
    SELECT
        *,
        INITCAP(TRIM(element)) element_title,
        COALESCE(promo, 'UNKNOWN') promo_clean
    FROM lines_carts
    WHERE created_at >= TIMESTAMP '2025-03-01 16:00:00'
),
scored AS (
    SELECT
        *,
        SUM(product_id) OVER w_running product_id_sum_15m,
        SUM(product_id) OVER w_running product_id_sum_15m_2,
        PERCENT_RANK() OVER (PARTITION BY element ORDER BY product_id DESC NULLS LAST) product_id_percent_rank,
        AVG(product_id) OVER w_running_2 product_id_avg_1h
    FROM base
    WINDOW w_running AS (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW), w_running_2 AS (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    SELECT
        *,
        CONCAT_WS('-', element, promo) element_promo_key,
        COALESCE(promo, 'N/A') resolved_promo
    FROM scored
    WHERE created_at >= TO_TIMESTAMP('2025-03-02 08:00:00')
      AND promo IS NOT NULL
),
scored_carts AS (
    SELECT
        *,
        ROUND(product_id_avg_1h / SUM(product_id_avg_1h) OVER (PARTITION BY element, promo), 4) product_id_avg_1h_share,
        PERCENT_RANK() OVER (PARTITION BY element ORDER BY product_id DESC) product_id_percent_rank_2
    FROM cleaned
),
filtered AS (
    SELECT
        promo,
        element,
        element_title,
        promo_clean,
        product_id_sum_15m,
        product_id_sum_15m_2,
        product_id_percent_rank,
        product_id_avg_1h,
        resolved_promo,
        product_id_avg_1h_share,
        product_id_percent_rank_2,
        YEAR(created_at) created_at_year,
        COALESCE(promo, 'OTHER') promo_filled
    FROM scored_carts
    WHERE created_at >= DATE '2025-03-01'
      AND element IS NOT NULL
      AND product_id_percent_rank < 0.47
)
SELECT
    CASE WHEN created_at_year >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END AS created_at_year_tier,
    CASE WHEN product_id_percent_rank >= 0.63 THEN 'LARGE' ELSE 'SMALL' END AS product_id_percent_rank_tier,
    COUNT(*) FILTER (WHERE element = 'WELCOME5') AS welcome5_rows,
    AVG(product_id_avg_1h) AS avg_product_id_avg_1h,
    RANK() OVER (PARTITION BY CASE WHEN created_at_year >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(*) FILTER (WHERE element = 'WELCOME5') DESC) AS welcome5_rows_rank
FROM filtered
GROUP BY CASE WHEN created_at_year >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN product_id_percent_rank >= 0.63 THEN 'LARGE' ELSE 'SMALL' END

WITH matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions s
    INNER JOIN promotions pr
        ON s.channel = pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on < pr.ends_on + INTERVAL '7' DAY
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(discount_pct AS INT) AS discount_pct_int,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM matched
    WHERE promo_code NOT IN ('BUNDLE', 'WELCOME5', 'VIP')
),
filtered_promotions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        discount_pct_int,
        ROUND(discount_pct_int / discount_pct, 2) AS discount_pct_int_per_discount_pct
    FROM filtered
    WHERE channel IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY promotion_promo_code, channel ORDER BY starts_on, id, promo_id) AS seq_no,
        SUM(discount_pct_int_per_discount_pct) OVER (PARTITION BY promo_code ORDER BY promotion_discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct_int_per_discount_pct,
        FIRST_VALUE(promotion_discount_pct, TRUE) OVER w_hist AS first_known_promotion_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY discount_pct_int_per_discount_pct DESC, id, promo_id) AS event_seq
    FROM filtered_promotions
    WINDOW w_hist AS (PARTITION BY promo_code ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    promo_id,
    promotion_promo_code,
    APPROX_PERCENTILE(discount_pct_int, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_int_quartiles,
    AVG(promo_id) AS avg_promo_id,
    COUNT(*) AS row_count
FROM with_windows
GROUP BY 1, 2;

WITH base AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        UPPER(promo_code) AS promo_code_upper
    FROM promotions
    WHERE channel IS NOT NULL
      AND promo_code = 'SPRING10'
),
base_promotions AS (
    SELECT
        id,
        channel,
        starts_on,
        discount_pct,
        promo_code_upper,
        CEIL(discount_pct) AS discount_pct_whole,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM base
    WHERE discount_pct < 0.22
),
matched_keys AS (
    SELECT id, channel
    FROM base_promotions
    INTERSECT ALL
    SELECT id, channel
    FROM promotions
    WHERE YEAR(starts_on) = 2023
),
kept_rows AS (
    SELECT s.*
    FROM base_promotions s
    INNER JOIN matched_keys k ON s.id = k.id AND s.channel = k.channel
),
with_window AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM kept_rows src
    INNER JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on < pr.ends_on + INTERVAL '7' DAY
),
with_window_promotions AS (
    SELECT
        s.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct_2
    FROM with_window s
    LEFT JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on < pr.ends_on + INTERVAL '14' DAY
),
attributed AS (
    SELECT
        a.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_3
    FROM with_window_promotions a
    LEFT JOIN promotions pr
        ON a.channel = pr.channel
       AND a.starts_on BETWEEN pr.starts_on AND pr.ends_on
)
SELECT
    DATE_TRUNC('WEEK', starts_on) AS week_start,
    promo_code,
    COUNT_IF(promotion_promo_code = 'VIP') AS vip_events,
    SUM(discount_pct_whole) FILTER (WHERE promotion_promo_code = 'BUNDLE') AS bundle_total_discount_pct_whole,
    RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', starts_on) ORDER BY SUM(discount_pct_whole) FILTER (WHERE promotion_promo_code = 'BUNDLE') DESC) AS bundle_total_discount_pct_whole_rank
FROM attributed
GROUP BY DATE_TRUNC('WEEK', starts_on), promo_code;

WITH base AS (
    SELECT
        *,
        UPPER(channel) AS channel_upper,
        ROUND(discount_pct * 0.9, 2) AS discount_pct_scaled
    FROM promotions
    WHERE channel IS DISTINCT FROM 'web'
      AND channel IS NOT NULL
),
matched AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM base src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'web') = pr.channel
       AND src.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
scored AS (
    SELECT
        *,
        AVG(discount_pct_scaled) OVER (PARTITION BY channel, promotion_promo_code) AS channel_avg_discount_pct_scaled,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct_scaled DESC, id, promo_id) AS event_seq,
        MIN(promo_id) OVER (PARTITION BY promotion_promo_code) AS promotion_promo_code_min_promo_id
    FROM matched
),
latest AS (
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY event_seq NULLS LAST, id, promo_id) = 1
)
SELECT *
FROM (
    SELECT
        promotion_promo_code,
        promo_code,
        discount_pct
    FROM latest
) src
PIVOT (
    AVG(discount_pct)
    FOR promo_code IN ('VIP' AS vip, 'SPRING10' AS spring10, 'WELCOME5' AS welcome5)
)

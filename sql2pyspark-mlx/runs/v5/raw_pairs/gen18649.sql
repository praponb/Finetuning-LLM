WITH filtered AS (
    SELECT
        *,
        CASE WHEN discount_pct >= 0.35 THEN 'High' WHEN discount_pct >= 0.13 THEN 'Medium' ELSE 'Low' END discount_pct_band,
        CONCAT(promo_code, ', ', channel) promo_code_channel
    FROM promotions
    WHERE promo_code IN ('SPRING10', 'WELCOME5')
),
attributed AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM filtered src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on <= DATE_ADD(pr.ends_on, 7)
),
with_window AS (
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM attributed e
    LEFT JOIN promotions pr
        ON COALESCE(e.channel, 'store') = pr.channel
       AND e.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
valid AS (
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct_band,
        promo_code_channel,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        promo_id_2,
        promotion_discount_pct_2,
        CAST(promo_id_2 AS STRING) promo_id_2_str,
        COALESCE(promotion_promo_code_2, 'DIRECT') resolved_promotion_promo_code_2
    FROM with_window
    WHERE promotion_promo_code_2 IS NOT NULL
),
matched AS (
    SELECT
        s.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct
    FROM valid s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on < pr.ends_on + INTERVAL '14' DAY
)
SELECT *
FROM (
    SELECT
        channel,
        promo_code,
        promotion_discount_pct_2
    FROM matched
) src
PIVOT (
    SUM(promotion_discount_pct_2) AS amount,
    COUNT(1) AS item_count
    FOR promo_code IN ('VIP' AS vip, 'BUNDLE' AS bundle)
)

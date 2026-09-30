WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        SUBSTRING(promo_code, 1, 4) promo_code_prefix,
        COALESCE(channel, 'OTHER') channel_filled
    FROM promotions
    WHERE starts_on >= DATE '2023-09-05'
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM filtered src
    JOIN promotions pr
        ON src.channel = pr.channel
       AND src.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
matched_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM matched src
    JOIN promotions pr
        ON COALESCE(src.channel, 'store') = pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on < pr.ends_on + INTERVAL 14 DAYS
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM matched_promotions src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'store') = pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on < pr.ends_on + INTERVAL '14' DAY
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        channel,
        promotion_promo_code,
        promo_id_3
    FROM with_window
) src
PIVOT (
    SUM(promo_id_3) AS value_sum,
    COUNT(1) AS cnt
    FOR promotion_promo_code IN ('WELCOME5' AS welcome5, 'BUNDLE' AS bundle, 'VIP' AS vip)
)

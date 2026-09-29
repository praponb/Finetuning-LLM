WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        CEIL(discount_pct) discount_pct_whole,
        COALESCE(channel, 'NONE') channel_filled
    FROM promotions
    WHERE ends_on >= DATE '2023-12-20'
      AND channel IS NOT NULL
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM base src
    LEFT JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM attributed s
    INNER JOIN promotions pr
        ON COALESCE(s.channel, 'web') = pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on <= DATE_ADD(pr.ends_on, 7)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        promotion_promo_code_2,
        channel,
        promo_code,
        promo_id
    FROM with_window
) src
PIVOT (
    MAX(promo_id) AS amount,
    COUNT(1) AS item_count
    FOR promo_code IN ('WELCOME5', 'BUNDLE')
);

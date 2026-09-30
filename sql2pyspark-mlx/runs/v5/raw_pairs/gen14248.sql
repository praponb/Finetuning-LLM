WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        SUBSTRING(promo_code, 1, 3) AS promo_code_prefix,
        UPPER(channel) AS channel_upper
    FROM promotions
    WHERE channel IS DISTINCT FROM 'store'
),
valid_promotions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        promo_code_prefix,
        channel_upper,
        CASE WHEN promo_code = 'BUNDLE' THEN 1 ELSE 0 END AS is_bundle
    FROM valid
    WHERE starts_on >= DATE '2023-05-20'
      AND channel != 'store'
),
valid_promotions_2 AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN promo_code IN ('SPRING10', 'BUNDLE') THEN 'Tier 1' ELSE 'Other' END AS promo_code_group,
        FLOOR(is_bundle) AS is_bundle_whole,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM valid_promotions
    WHERE promo_code IN ('WELCOME5', 'BUNDLE', 'VIP')
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM valid_promotions_2 src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on <= DATE_ADD(pr.ends_on, 3)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        channel,
        promo_code,
        is_bundle_whole
    FROM with_window
) src
PIVOT (
    AVG(is_bundle_whole)
    FOR promo_code IN ('VIP', 'BUNDLE')
);

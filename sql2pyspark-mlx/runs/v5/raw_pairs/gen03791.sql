WITH valid AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        DATE_ADD(ends_on, 66) AS ends_on_plus_66d,
        ROUND(discount_pct * 100, 2) AS discount_pct_adjusted,
        COALESCE(channel, 'DIRECT') AS channel_filled
    FROM promotions
    WHERE ends_on >= DATE '2023-11-06'
)
SELECT
    src.*,
    pr.id AS promo_id,
    pr.promo_code AS promotion_promo_code,
    pr.discount_pct AS promotion_discount_pct
FROM valid src
LEFT JOIN promotions pr
    ON src.channel <=> pr.channel
   AND src.starts_on BETWEEN pr.starts_on AND pr.ends_on;

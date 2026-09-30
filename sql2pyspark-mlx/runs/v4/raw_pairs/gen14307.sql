WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(channel)) AS channel_title,
        COALESCE(channel, 'OTHER') AS channel_filled
    FROM promotions
    WHERE ends_on BETWEEN '2024-01-01' AND '2025-07-01'
)
-- Non-equi join on a validity window
SELECT
    e.*,
    pr.id AS promo_id,
    pr.promo_code AS promotion_promo_code,
    pr.discount_pct AS promotion_discount_pct
FROM cleaned e
JOIN promotions pr
    ON COALESCE(e.channel, 'web') = pr.channel
   AND e.starts_on >= pr.starts_on
   AND e.starts_on < pr.ends_on + INTERVAL '14' DAY;

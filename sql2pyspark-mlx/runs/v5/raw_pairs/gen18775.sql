WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(channel)) AS channel_title,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM promotions
    WHERE channel IS NOT NULL
)
-- Ranking and gap features
SELECT
    *,
    LAG(discount_pct, 1) OVER (PARTITION BY promo_code ORDER BY ends_on, id) AS prev_discount_pct,
    MAX(discount_pct) OVER (PARTITION BY promo_code, channel) AS promo_code_max_discount_pct,
    LAG(discount_pct, 1, 0) OVER (PARTITION BY channel ORDER BY starts_on, id) AS prev_discount_pct_2
FROM filtered

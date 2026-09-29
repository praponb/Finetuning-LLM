WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_TRUNC('year', starts_on) AS starts_on_period,
        COALESCE(channel, 'DIRECT') AS channel_clean
    FROM promotions
    WHERE channel IS DISTINCT FROM 'store'
      AND channel IS NOT NULL
)
-- Keep one row per key (latest first)
SELECT
    id,
    channel,
    discount_pct,
    starts_on_period,
    channel_clean
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY starts_on_period DESC NULLS LAST, id) AS rank_in_group
    FROM base
) t
WHERE rank_in_group = 1

WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(channel, 1, 2) AS channel_prefix
    FROM promotions
    WHERE channel IN ('web', 'store', 'partner')
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        promo_code,
        channel,
        discount_pct
    FROM filtered
) src
PIVOT (
    MAX(discount_pct) AS amount,
    COUNT(1) AS cnt
    FOR channel IN ('web', 'partner')
)

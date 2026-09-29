WITH remaining_keys AS (
    SELECT channel, promo_code
    FROM promotions
    EXCEPT DISTINCT
    SELECT channel, promo_code
    FROM promotions
    WHERE promo_code IN ('BUNDLE', 'VIP', 'SPRING10')
),
kept_rows AS (
    SELECT s.*
    FROM promotions s
    INNER JOIN remaining_keys k ON s.channel = k.channel AND s.promo_code = k.promo_code
)
SELECT
    channel,
    promo_code,
    starts_on
FROM kept_rows
ORDER BY promo_code NULLS FIRST, starts_on DESC;

WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(promo_code, 1, 4) promo_code_prefix
    FROM promotions
    WHERE channel IS NOT NULL
)
SELECT id, channel
FROM cleaned
EXCEPT
SELECT id, channel
FROM promotions
WHERE promo_code IN ('BUNDLE', 'WELCOME5', 'SPRING10')

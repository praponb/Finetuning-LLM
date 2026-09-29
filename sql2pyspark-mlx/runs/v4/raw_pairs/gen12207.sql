WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(discount_pct) AS discount_pct_whole,
        UPPER(promo_code) AS promo_code_upper
    FROM promotions
    WHERE ends_on > DATE '2023-10-04'
      AND channel IS NOT NULL
)
SELECT channel, promo_code
FROM base
EXCEPT ALL
SELECT channel, promo_code
FROM promotions
WHERE discount_pct < 0.17;

WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        promo_code,
        starts_on,
        discount_pct,
        CAST(discount_pct AS INT) AS discount_pct_int
    FROM promotions
    WHERE ends_on > '2025-09-01'
      AND channel IS NOT NULL
)
SELECT id
FROM filtered
EXCEPT DISTINCT
SELECT id
FROM promotions
WHERE discount_pct >= 0.24;

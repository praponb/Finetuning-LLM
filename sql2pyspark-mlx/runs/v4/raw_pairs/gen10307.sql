WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(promo_code) AS promo_code_lower
    FROM promotions
    WHERE ends_on >= DATE '2023-12-13'
      AND channel IS NOT NULL
)
-- Deduplicate with ROW_NUMBER
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on DESC, id) AS rank_in_group
    FROM valid
) x
WHERE rank_in_group = 1;

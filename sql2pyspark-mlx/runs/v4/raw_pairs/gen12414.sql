WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        DATE_FORMAT(starts_on, 'yyyy-MM-dd') starts_on_label
    FROM promotions
    WHERE starts_on >= DATE '2023-10-06'
      AND ends_on <= '2025-08-08'
)
-- QUALIFY-style window filter
SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on, id) AS rank_in_group
FROM base
QUALIFY rank_in_group = 1

WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM promotions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY starts_on DESC, id) = 1
),
ranked_promotions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY starts_on DESC, id) = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT promo_code, channel
    FROM ranked_promotions
    EXCEPT
    SELECT promo_code, channel
    FROM promotions
    WHERE starts_on > '2023-03-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked_promotions s
    JOIN matched_keys k ON s.promo_code = k.promo_code AND s.channel = k.channel
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ADD_MONTHS(ends_on, 12) AS ends_on_plus_months
    FROM kept_rows
    WHERE ends_on >= DATE '2023-09-21'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, promo_code
    FROM valid
    EXCEPT
    SELECT id, promo_code
    FROM promotions
    WHERE promo_code IN ('WELCOME5', 'SPRING10')
),
kept_rows_promotions AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN remaining_keys k ON s.id = k.id AND s.promo_code = k.promo_code
)
SELECT
    ends_on,
    discount_pct,
    ends_on_plus_months
FROM kept_rows_promotions
ORDER BY discount_pct DESC;

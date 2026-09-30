WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        YEAR(ends_on) AS ends_on_year,
        ADD_MONTHS(starts_on, 12) AS starts_on_plus_months
    FROM promotions
    WHERE channel IS NOT NULL
      AND ends_on >= '2023-08-01'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(ends_on_year, 2) AS ends_on_year_rounded
    FROM cleaned
    WHERE channel IS NOT NULL
      AND ends_on <= '2025-06-01'
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM filtered src
    LEFT JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on < pr.ends_on + INTERVAL 30 DAYS
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, promo_code
    FROM matched
    INTERSECT
    SELECT id, promo_code
    FROM promotions
    WHERE starts_on BETWEEN '2025-03-02' AND '2025-08-25'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM matched s
    INNER JOIN kept_keys k ON s.id = k.id AND s.promo_code = k.promo_code
)
SELECT
    id,
    channel,
    starts_on_plus_months,
    ends_on_year_rounded
FROM kept_rows
ORDER BY id NULLS FIRST;

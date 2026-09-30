WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        EXPLODE_OUTER(discounts) AS element
    FROM price_tiers
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(element * 100, 2) AS element_adjusted
    FROM lines
    WHERE currency IN ('THB', 'USD')
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN currency = 'USD' THEN 1 ELSE 0 END AS is_usd
    FROM filtered
    WHERE currency = 'THB'
)
-- Running and moving aggregates
SELECT
    *,
    PERCENT_RANK() OVER (ORDER BY is_usd NULLS LAST) AS is_usd_percent_rank,
    CUME_DIST() OVER (PARTITION BY currency ORDER BY is_usd NULLS FIRST) AS is_usd_cume_dist,
    MIN(is_usd) OVER w_running AS currency_min_is_usd,
    MIN(element_adjusted) OVER w_running AS currency_min_element_adjusted
FROM cleaned
WINDOW w_running AS (PARTITION BY currency);

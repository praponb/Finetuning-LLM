WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS element
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        effective_from,
        currency,
        element,
        DENSE_RANK() OVER (ORDER BY element DESC) AS element_dense_rank,
        AVG(element) OVER (PARTITION BY currency) AS currency_avg_element
    FROM lines
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        effective_from,
        currency,
        element,
        element_dense_rank,
        currency_avg_element,
        element / NULLIF(currency_avg_element, 0) AS element_per_currency_avg_element,
        CASE WHEN element_dense_rank >= 7 THEN 'High' WHEN element_dense_rank >= 3 THEN 'Medium' ELSE 'Low' END AS element_dense_rank_band
    FROM with_windows
    WHERE element BETWEEN 46 AND 72
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        currency,
        currency_avg_element,
        element_per_currency_avg_element,
        element_dense_rank_band,
        PERCENT_RANK() OVER (ORDER BY element DESC) AS element_percent_rank,
        ROUND(element_dense_rank / SUM(element_dense_rank) OVER (PARTITION BY currency), 4) AS element_dense_rank_share
    FROM valid
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    currency,
    kpi,
    value
FROM (
    SELECT
        currency,
        element_dense_rank_share,
        element_per_currency_avg_element,
        element_percent_rank
    FROM metrics
) src
UNPIVOT (
    value FOR kpi IN (element_dense_rank_share, element_per_currency_avg_element, element_percent_rank)
) long_rows

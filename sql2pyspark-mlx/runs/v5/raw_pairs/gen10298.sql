WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) li AS element
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(currency) AS currency_upper
    FROM lines
    WHERE element > 64
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        currency,
        element,
        currency_upper,
        SUBSTRING(currency, 1, 2) AS currency_prefix,
        DATE_FORMAT(effective_from, 'yyyy-MM-dd') AS effective_from_label
    FROM filtered
    WHERE element <= 49
),
filtered_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(element, 1) AS element_rounded,
        CASE WHEN element >= 60 THEN 'High' WHEN element >= 31 THEN 'Medium' ELSE 'Low' END AS element_tier
    FROM base
    WHERE element >= 20
),
filtered_price_tiers_2 AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(element_rounded) AS element_rounded_whole,
        element + element_rounded AS element_plus_element_rounded
    FROM filtered_price_tiers
    WHERE currency IN ('EUR', 'THB')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        currency,
        element,
        currency_upper,
        effective_from_label,
        element_rounded,
        element_plus_element_rounded,
        RANK() OVER (PARTITION BY currency ORDER BY element NULLS LAST) AS element_rank,
        MIN(element_plus_element_rounded) OVER w_part AS currency_min_element_plus_element_rounded,
        RANK() OVER (ORDER BY element_plus_element_rounded NULLS LAST) AS element_plus_element_rounded_rank
    FROM filtered_price_tiers_2
    WINDOW w_part AS (PARTITION BY currency)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY element_plus_element_rounded DESC NULLS FIRST) AS element_plus_element_rounded_cume_dist,
        MIN(element_rank) OVER (PARTITION BY currency) AS currency_min_element_rank,
        DENSE_RANK() OVER (ORDER BY element_plus_element_rounded_rank DESC) AS element_plus_element_rounded_rank_dense_rank,
        ROUND(element_rank / SUM(element_rank) OVER (PARTITION BY currency), 4) AS element_rank_share
    FROM metrics
    WHERE element_rounded < 58
)
SELECT
    currency,
    element,
    currency_upper,
    effective_from_label,
    element_rank,
    element_plus_element_rounded_rank,
    element_plus_element_rounded_rank_dense_rank,
    element_rank_share
FROM enriched
ORDER BY element DESC NULLS FIRST, currency

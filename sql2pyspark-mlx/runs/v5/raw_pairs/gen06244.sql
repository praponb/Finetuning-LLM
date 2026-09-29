WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        EXPLODE_OUTER(tier_prices) AS element
    FROM price_tiers
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(currency, 1, 3) currency_prefix
    FROM lines
    WHERE element > 47
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(element, 0) element_filled
    FROM base
    WHERE currency = 'THB'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        currency,
        CUME_DIST() OVER (ORDER BY element_filled DESC NULLS FIRST) element_filled_cume_dist,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY element_filled DESC NULLS FIRST) element_filled_cume_dist_2,
        MIN(element_filled) OVER (PARTITION BY currency) currency_min_element_filled
    FROM cleaned
    WHERE currency NOT IN ('EUR', 'THB')
)
-- Window metrics over several different partitions
SELECT
    *,
    PERCENT_RANK() OVER (ORDER BY element_filled_cume_dist_2) element_filled_cume_dist_2_percent_rank,
    DENSE_RANK() OVER (PARTITION BY currency ORDER BY element_filled_cume_dist_2 DESC) element_filled_cume_dist_2_dense_rank
FROM metrics

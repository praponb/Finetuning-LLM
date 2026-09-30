WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS element
    FROM price_tiers
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER w_hist element_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY element NULLS FIRST) element_percent_rank,
        CUME_DIST() OVER (ORDER BY element DESC) element_cume_dist,
        RANK() OVER w_hist element_rank_2,
        PERCENT_RANK() OVER w_hist_2 element_percent_rank_2
    FROM exploded
    WINDOW w_hist AS (PARTITION BY currency ORDER BY element DESC NULLS LAST), w_hist_2 AS (PARTITION BY currency ORDER BY element DESC)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        effective_from,
        currency,
        element,
        element_rank,
        element_percent_rank,
        element_cume_dist,
        element_rank_2,
        element_percent_rank_2,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY element_cume_dist DESC) element_cume_dist_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY element_percent_rank_2 DESC) element_percent_rank_2_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY element) element_percent_rank_3
    FROM metrics
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        effective_from,
        currency,
        element,
        element_rank,
        element_percent_rank,
        element_rank_2,
        element_percent_rank_2,
        element_cume_dist_cume_dist,
        element_percent_rank_2_percent_rank,
        element_percent_rank_3,
        element_rank / NULLIF(element_percent_rank_3, 0) element_rank_per_element_percent_rank_3
    FROM enriched
    WHERE element_cume_dist <= 0.8
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, element_percent_rank_2 / NULLIF(element_rank_per_element_percent_rank_3, 0) element_percent_rank_2_per_element_rank_per_element_percent_rank_3
    FROM filtered
    WHERE effective_from > DATE '2024-10-29'
),
filtered_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(currency) currency_upper
    FROM valid
    WHERE effective_from > TO_DATE('2024-03-17')
),
enriched_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY currency ORDER BY element_percent_rank_2) element_percent_rank_2_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY element_percent_rank_2_per_element_rank_per_element_percent_rank_3) element_percent_rank_2_per_element_rank_per_element_percent_rank_3_percent_rank,
        PERCENT_RANK() OVER w_running element_percent_rank_percent_rank
    FROM filtered_price_tiers
    WINDOW w_running AS (ORDER BY element_percent_rank DESC NULLS FIRST)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        currency,
        element_percent_rank_percent_rank,
        element_percent_rank_2_per_element_rank_per_element_percent_rank_3_percent_rank,
        element,
        element_percent_rank_2
    FROM enriched_price_tiers
) src
UNPIVOT (
    reading FOR metric_name IN (element_percent_rank_percent_rank AS ELEMENT_PERCENT_RANK_PERCENT_RANK_VAL, element_percent_rank_2_per_element_rank_per_element_percent_rank_3_percent_rank AS ELEMENT_PERCENT_RANK_2_PER_ELEMENT_RANK_PER_ELEMENT_PERCENT_RANK_3_PERCENT_RANK_VAL, element AS ELEMENT_VAL, element_percent_rank_2)
)

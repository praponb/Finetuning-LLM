WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS element
    FROM price_tiers
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        MIN(element) OVER (PARTITION BY currency) AS currency_min_element,
        CUME_DIST() OVER (ORDER BY element DESC NULLS LAST) AS element_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY element DESC NULLS LAST) AS element_percent_rank
    FROM flattened
    WHERE effective_from <= '2023-08-01'
),
scored AS (
    -- Ranking and gap features
    SELECT *, DENSE_RANK() OVER (PARTITION BY plan ORDER BY element_percent_rank NULLS FIRST) AS element_percent_rank_dense_rank
    FROM enriched
),
scored_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY element_percent_rank DESC) AS element_percent_rank_rank,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY element_percent_rank NULLS LAST) AS element_percent_rank_percent_rank,
        CUME_DIST() OVER (ORDER BY currency_min_element DESC NULLS FIRST) AS currency_min_element_cume_dist
    FROM scored
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(element_percent_rank_rank) AS element_percent_rank_rank_whole
    FROM scored_price_tiers
    WHERE element_cume_dist <= 0.39
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        currency_min_element,
        element_cume_dist,
        element_percent_rank,
        element_percent_rank_rank,
        currency_min_element_cume_dist,
        element_percent_rank_rank_whole,
        element_percent_rank_dense_rank - element_percent_rank_rank_whole AS element_percent_rank_dense_rank_minus_element_percent_rank_rank_whole
    FROM valid
    WHERE element_percent_rank >= 0.67
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN element_percent_rank_dense_rank_minus_element_percent_rank_rank_whole >= 62 THEN 'High' WHEN element_percent_rank_dense_rank_minus_element_percent_rank_rank_whole >= 12 THEN 'Medium' ELSE 'Low' END AS element_percent_rank_dense_rank_minus_element_percent_rank_rank_whole_level
    FROM base
    WHERE element_percent_rank_dense_rank_minus_element_percent_rank_rank_whole >= 29
)
-- Grouped metrics
SELECT
    plan,
    CASE WHEN element_percent_rank >= 0.63 THEN 'PREMIUM' ELSE 'STANDARD' END AS element_percent_rank_tier,
    APPROX_PERCENTILE(element_cume_dist, 0.5) AS element_cume_dist_p50,
    ROUND(AVG(element_cume_dist), 2) AS avg_element_cume_dist,
    COUNT(DISTINCT plan) AS distinct_plans,
    RANK() OVER (PARTITION BY plan ORDER BY ROUND(AVG(element_cume_dist), 2) DESC) AS avg_element_cume_dist_rank
FROM cleaned
GROUP BY plan, element_percent_rank_tier
HAVING COUNT(DISTINCT plan) < 1000;

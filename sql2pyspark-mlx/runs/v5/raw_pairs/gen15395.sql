WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_limits) AS element
    FROM price_tiers
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, ADD_MONTHS(effective_from, 3) effective_from_plus_months
    FROM exploded
    WHERE effective_from >= DATE '2024-04-11'
      AND element < 79
),
enriched AS (
    -- Ranking and gap features
    SELECT
        effective_from,
        effective_from_plus_months,
        COUNT(element) OVER (PARTITION BY currency) currency_count_element,
        DENSE_RANK() OVER (ORDER BY element DESC NULLS FIRST) element_dense_rank,
        AVG(element) OVER (PARTITION BY currency) currency_avg_element
    FROM base
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY currency_count_element DESC NULLS LAST) currency_count_element_cume_dist,
        RANK() OVER (ORDER BY currency_avg_element DESC NULLS FIRST) currency_avg_element_rank
    FROM enriched
    WHERE effective_from BETWEEN '2024-07-01' AND '2025-09-21'
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    PERCENTILE_APPROX(currency_avg_element_rank, ARRAY(0.25, 0.5, 0.75)) AS currency_avg_element_rank_quartiles,
    APPROX_PERCENTILE(currency_avg_element_rank, 0.5) AS currency_avg_element_rank_p50,
    PERCENTILE_APPROX(currency_avg_element_rank, 0.9) AS currency_avg_element_rank_p90
FROM metrics
GROUP BY DATE_TRUNC('WEEK', effective_from)
HAVING currency_avg_element_rank_p90 > 0
ORDER BY currency_avg_element_rank_p50 NULLS LAST

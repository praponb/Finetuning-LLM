WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_limits) AS element
    FROM price_tiers
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        effective_from,
        currency,
        element,
        CUME_DIST() OVER (PARTITION BY plan, currency ORDER BY element NULLS FIRST) AS element_cume_dist,
        RANK() OVER (PARTITION BY currency ORDER BY element DESC) AS element_rank,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY element DESC) AS element_dense_rank,
        RANK() OVER (PARTITION BY currency ORDER BY element NULLS LAST) AS element_rank_2
    FROM lines
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_ADD(effective_from, 82) AS effective_from_plus_82d,
        GREATEST(element_dense_rank, element) AS greatest_element_dense_rank_element
    FROM with_windows
    WHERE currency = 'THB'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        effective_from,
        currency,
        element,
        element_rank,
        element_dense_rank,
        effective_from_plus_82d,
        greatest_element_dense_rank_element,
        SUM(element_cume_dist) OVER (PARTITION BY currency) AS currency_sum_element_cume_dist
    FROM valid
),
totals AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('WEEK', effective_from_plus_82d) AS week_start,
        CASE WHEN element_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS element_rank_tier,
        SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS num_rows
    FROM metrics
    GROUP BY DATE_TRUNC('WEEK', effective_from_plus_82d), CASE WHEN element_rank >= 7 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    week_start,
    element_rank_tier,
    currency_list,
    distinct_ids
FROM totals
ORDER BY distinct_ids DESC NULLS LAST, week_start;

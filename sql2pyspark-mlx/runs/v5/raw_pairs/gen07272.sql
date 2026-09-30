WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE(tier_limits) AS element
    FROM price_tiers
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(element) OVER w_running AS plan_count_element,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY element DESC NULLS FIRST) AS element_percent_rank,
        STDDEV_SAMP(element) OVER w_running AS plan_stddev_samp_element,
        CUME_DIST() OVER w_ordered AS element_cume_dist,
        COUNT(element) OVER w_running AS plan_count_element_2
    FROM flattened
    WINDOW w_running AS (PARTITION BY plan), w_ordered AS (PARTITION BY plan ORDER BY element NULLS FIRST)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        element,
        plan_count_element,
        element_percent_rank,
        plan_stddev_samp_element,
        MAX(plan_stddev_samp_element) OVER (PARTITION BY plan) AS plan_max_plan_stddev_samp_element,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY element_percent_rank DESC) AS element_percent_rank_dense_rank
    FROM scored
    WHERE plan_count_element IS NULL
),
matched_keys AS (
    -- Set intersection on keys
    SELECT plan, id
    FROM with_windows
    INTERSECT ALL
    SELECT plan, id
    FROM price_tiers
    WHERE plan = 'basic'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.plan = k.plan AND s.id = k.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, LEAST(plan_count_element, plan_max_plan_stddev_samp_element) AS least_plan_count_element_plan_max_plan_stddev_samp_element
    FROM reconciled
    WHERE plan_count_element < 14
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        element,
        plan_count_element,
        plan_stddev_samp_element,
        plan_max_plan_stddev_samp_element,
        least_plan_count_element_plan_max_plan_stddev_samp_element,
        RANK() OVER (PARTITION BY plan ORDER BY plan_count_element DESC NULLS LAST) AS plan_count_element_rank,
        COUNT(element_percent_rank) OVER (PARTITION BY plan) AS plan_count_element_percent_rank,
        STDDEV_SAMP(plan_count_element) OVER (PARTITION BY plan) AS plan_stddev_samp_plan_count_element,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY element DESC NULLS LAST) AS element_cume_dist
    FROM filtered
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(plan_stddev_samp_element) AS plan_stddev_samp_element_whole
    FROM enriched
    WHERE plan_count_element_percent_rank = 12
)
-- Grouped metrics
SELECT
    CASE WHEN plan_stddev_samp_element >= 83 THEN 'PREMIUM' ELSE 'STANDARD' END AS plan_stddev_samp_element_tier,
    COUNT(*) AS cnt,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(element_cume_dist, 0.95) AS element_cume_dist_p95
FROM base
GROUP BY CASE WHEN plan_stddev_samp_element >= 83 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY cnt DESC

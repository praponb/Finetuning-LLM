WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        effective_from,
        currency,
        tier_value,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value DESC) AS tier_value_cume_dist,
        RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value) AS tier_value_percent_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_percent_rank_2
    FROM flattened
),
kept_keys AS (
    -- Set difference on keys
    SELECT currency, id
    FROM scored
    EXCEPT
    SELECT currency, id
    FROM price_tiers
    WHERE plan = 'basic'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN kept_keys k ON s.currency = k.currency AND s.id = k.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN tier_value_percent_rank >= 0.86 THEN 'High' WHEN tier_value_percent_rank >= 0.13 THEN 'Medium' ELSE 'Low' END AS tier_value_percent_rank_level
    FROM verified
    WHERE tier_value_percent_rank_2 BETWEEN 0.53 AND 0.55
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        effective_from,
        currency,
        tier_value,
        tier_value_cume_dist,
        tier_value_rank,
        tier_value_percent_rank,
        tier_value_percent_rank_level,
        tier_value_percent_rank_2 * tier_value AS tier_value_percent_rank_2_times_tier_value,
        LOWER(currency) AS currency_lower
    FROM cleaned
    WHERE effective_from > DATE '2024-09-04'
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    currency,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    ROUND(AVG(tier_value), 2) AS avg_tier_value,
    PERCENTILE_APPROX(tier_value_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS tier_value_cume_dist_quartiles,
    SIZE(COLLECT_SET(currency)) AS num_currencies,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', effective_from) ORDER BY ROUND(AVG(tier_value), 2) DESC) AS avg_tier_value_percent_rank
FROM base
GROUP BY 1, 2;

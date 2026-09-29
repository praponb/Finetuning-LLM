WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS val
    FROM price_tiers
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY val NULLS FIRST) val_percent_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val DESC NULLS FIRST) val_cume_dist
    FROM lines
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(effective_from, 'MMM yyyy') effective_from_label
    FROM enriched
    WHERE effective_from >= DATE '2024-06-16'
      AND val_cume_dist > 0.69
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(plan, ', ', currency) plan_currency,
        SUBSTRING(plan, 1, 4) plan_prefix
    FROM valid
    WHERE effective_from >= DATE '2024-03-24'
)
-- Grouped metrics
SELECT
    CASE WHEN val >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END AS val_tier,
    currency,
    APPROX_PERCENTILE(val_percent_rank, 0.5) AS val_percent_rank_p50,
    COUNT(*) FILTER (WHERE plan = 'enterprise') AS enterprise_rows,
    ROUND(SUM(val_cume_dist) / NULLIF(COUNT(CASE WHEN plan = 'pro' THEN 1 END), 0), 2) AS val_cume_dist_per_pro,
    COUNT(DISTINCT plan) AS distinct_plans,
    COUNT(DISTINCT currency) AS distinct_currencies,
    AVG(val_cume_dist) FILTER (WHERE currency = 'EUR') AS eur_avg_val_cume_dist
FROM filtered
GROUP BY 1, 2
ORDER BY eur_avg_val_cume_dist

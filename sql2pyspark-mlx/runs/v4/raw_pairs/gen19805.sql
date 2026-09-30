WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) x AS val
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        effective_from,
        val,
        RANK() OVER (PARTITION BY plan ORDER BY val DESC NULLS FIRST) AS val_rank,
        DENSE_RANK() OVER (ORDER BY val DESC NULLS FIRST) AS val_dense_rank
    FROM lines
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        plan,
        effective_from,
        val,
        val_rank,
        val_dense_rank,
        DATE_FORMAT(effective_from, 'MMM yyyy') AS effective_from_label,
        SUBSTRING(plan, 1, 4) AS plan_prefix
    FROM enriched
    WHERE val_rank IS NOT NULL
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    AVG(val_rank) FILTER (WHERE plan = 'enterprise') AS enterprise_avg_val_rank,
    COUNT(*) FILTER (WHERE plan = 'starter') AS starter_rows
FROM cleaned
GROUP BY 1
ORDER BY activity_month NULLS FIRST;

WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) t AS val
),
enriched AS (
    SELECT
        *,
        AVG(val) OVER (PARTITION BY plan, currency) plan_avg_val,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY val DESC NULLS LAST) val_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val DESC) val_percent_rank
    FROM lines
),
cleaned AS (
    SELECT *, CONCAT_WS('-', currency, plan) currency_plan_key
    FROM enriched
    WHERE val_cume_dist BETWEEN 0.59 AND 0.77
),
scored AS (
    SELECT
        *,
        MIN(val) OVER (PARTITION BY plan) plan_min_val,
        DENSE_RANK() OVER (PARTITION BY currency, plan ORDER BY val DESC) val_dense_rank
    FROM cleaned
)
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    currency,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows,
    PERCENTILE_APPROX(val_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS val_percent_rank_quartiles
FROM scored
GROUP BY CUBE (DATE_TRUNC('QUARTER', effective_from), currency)
ORDER BY basic_rows NULLS LAST;

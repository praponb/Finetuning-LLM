WITH lines AS (
    SELECT
        id,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) x AS tier_value
),
valid AS (
    SELECT *, COALESCE(tier_value, 0) AS tier_value_filled
    FROM lines
    WHERE effective_from BETWEEN '2023-08-21' AND '2024-02-05'
),
base AS (
    SELECT *, DATE_TRUNC('week', effective_from) AS effective_from_period
    FROM valid
    WHERE effective_from > DATE '2024-06-26'
),
enriched AS (
    SELECT
        id,
        effective_from,
        tier_value,
        tier_value_filled,
        effective_from_period,
        PERCENT_RANK() OVER (ORDER BY tier_value_filled DESC NULLS FIRST) AS tier_value_filled_percent_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value_filled NULLS LAST) AS tier_value_filled_percent_rank_2,
        CUME_DIST() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_cume_dist
    FROM base
),
enriched_price_tiers AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY tier_value_cume_dist DESC NULLS FIRST) AS tier_value_cume_dist_percent_rank,
        SUM(tier_value) OVER (ORDER BY CAST(effective_from_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS tier_value_sum_90d
    FROM enriched
),
cleaned AS (
    SELECT
        effective_from,
        tier_value,
        tier_value_filled,
        effective_from_period,
        tier_value_filled_percent_rank,
        tier_value_cume_dist,
        tier_value_sum_90d,
        DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from
    FROM enriched_price_tiers
    WHERE effective_from > DATE '2024-09-18'
      AND tier_value > 72
),
enriched_price_tiers_2 AS (
    SELECT
        *,
        RANK() OVER (ORDER BY tier_value_sum_90d) AS tier_value_sum_90d_rank,
        PERCENT_RANK() OVER w_ordered AS tier_value_percent_rank,
        COUNT(days_since_effective_from) OVER (ORDER BY CAST(effective_from_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS days_since_effective_from_count_30d,
        PERCENT_RANK() OVER (ORDER BY days_since_effective_from DESC) AS days_since_effective_from_percent_rank,
        DENSE_RANK() OVER (ORDER BY tier_value_filled NULLS LAST) AS tier_value_filled_dense_rank
    FROM cleaned
    WINDOW w_ordered AS (ORDER BY tier_value DESC)
)
SELECT
    CASE WHEN tier_value_filled_percent_rank >= 0.77 THEN 'HIGH' ELSE 'LOW' END AS tier_value_filled_percent_rank_tier,
    (UNIX_TIMESTAMP(MAX(effective_from_period)) - UNIX_TIMESTAMP(MIN(effective_from_period))) / 60 AS duration_min,
    PERCENTILE_APPROX(tier_value_percent_rank, 0.5) AS tier_value_percent_rank_p50,
    ROUND(AVG(tier_value_filled), 2) AS avg_tier_value_filled
FROM enriched_price_tiers_2
GROUP BY CASE WHEN tier_value_filled_percent_rank >= 0.77 THEN 'HIGH' ELSE 'LOW' END
ORDER BY avg_tier_value_filled DESC

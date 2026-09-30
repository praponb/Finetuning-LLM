WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) li AS tier_value
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        effective_from,
        tier_value,
        RANK() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_rank,
        DENSE_RANK() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_dense_rank,
        DENSE_RANK() OVER (ORDER BY tier_value) AS tier_value_dense_rank_2
    FROM lines
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER w_ordered AS tier_value_rank_percent_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value_dense_rank_2 DESC) AS tier_value_dense_rank_2_percent_rank,
        DENSE_RANK() OVER w_ordered AS tier_value_rank_dense_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_percent_rank,
        DENSE_RANK() OVER (ORDER BY tier_value_dense_rank_2 DESC NULLS LAST) AS tier_value_dense_rank_2_dense_rank
    FROM with_windows
    WINDOW w_ordered AS (ORDER BY tier_value_rank)
)
SELECT
    id,
    tier_value_rank,
    tier_value_dense_rank,
    tier_value_dense_rank_2,
    tier_value_rank_percent_rank,
    tier_value_dense_rank_2_percent_rank,
    tier_value_rank_dense_rank,
    tier_value_percent_rank,
    tier_value_dense_rank_2_dense_rank
FROM enriched
ORDER BY tier_value_rank DESC

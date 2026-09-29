WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT *, DENSE_RANK() OVER (ORDER BY tier_value NULLS FIRST) tier_value_dense_rank
    FROM lines
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY tier_value NULLS FIRST) tier_value_percent_rank,
        RANK() OVER (ORDER BY tier_value_dense_rank DESC NULLS FIRST) tier_value_dense_rank_rank
    FROM metrics
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        tier_value,
        tier_value_percent_rank,
        CAST(tier_value_percent_rank AS INT) tier_value_percent_rank_int
    FROM enriched
    WHERE effective_from > DATE '2024-02-26'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        tier_value,
        CUME_DIST() OVER (ORDER BY tier_value_percent_rank DESC) tier_value_percent_rank_cume_dist,
        PERCENT_RANK() OVER (ORDER BY tier_value_percent_rank_int NULLS LAST) tier_value_percent_rank_int_percent_rank,
        CUME_DIST() OVER (ORDER BY tier_value) tier_value_cume_dist
    FROM filtered
),
scored_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY tier_value_percent_rank_cume_dist NULLS LAST) tier_value_percent_rank_cume_dist_percent_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value_percent_rank_int_percent_rank NULLS FIRST) tier_value_percent_rank_int_percent_rank_percent_rank
    FROM scored
),
scored_price_tiers_2 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        tier_value,
        tier_value_percent_rank_cume_dist,
        tier_value_percent_rank_int_percent_rank,
        tier_value_percent_rank_cume_dist_percent_rank,
        tier_value_percent_rank_int_percent_rank_percent_rank,
        CUME_DIST() OVER (ORDER BY tier_value_percent_rank_cume_dist_percent_rank DESC NULLS FIRST) tier_value_percent_rank_cume_dist_percent_rank_cume_dist,
        DENSE_RANK() OVER (ORDER BY tier_value_cume_dist DESC NULLS FIRST) tier_value_cume_dist_dense_rank
    FROM scored_price_tiers
    WHERE tier_value_percent_rank_int_percent_rank_percent_rank BETWEEN 0.23 AND 0.8
)
SELECT
    id,
    tier_value,
    tier_value_percent_rank_int_percent_rank,
    tier_value_percent_rank_int_percent_rank_percent_rank,
    tier_value_percent_rank_cume_dist_percent_rank_cume_dist,
    tier_value_cume_dist_dense_rank
FROM scored_price_tiers_2
ORDER BY tier_value DESC NULLS FIRST;

WITH flattened AS (
    SELECT id, EXPLODE(discounts) AS tier_value
    FROM price_tiers
),
scored AS (
    SELECT
        id,
        tier_value,
        RANK() OVER w_running AS tier_value_rank,
        CUME_DIST() OVER (ORDER BY tier_value NULLS FIRST) AS tier_value_cume_dist,
        DENSE_RANK() OVER w_recent AS tier_value_dense_rank
    FROM flattened
    WINDOW w_running AS (ORDER BY tier_value), w_recent AS (ORDER BY tier_value NULLS LAST)
),
matched_keys AS (
    SELECT id
    FROM scored
    EXCEPT
    SELECT id
    FROM price_tiers
    WHERE currency IN ('THB', 'EUR', 'USD')
),
reconciled AS (
    SELECT s.*
    FROM scored s
    JOIN matched_keys k ON s.id = k.id
),
with_windows AS (
    SELECT
        id,
        tier_value,
        tier_value_rank,
        tier_value_cume_dist,
        tier_value_dense_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value_cume_dist DESC NULLS LAST) AS tier_value_cume_dist_percent_rank,
        RANK() OVER (ORDER BY tier_value_cume_dist DESC NULLS FIRST) AS tier_value_cume_dist_rank,
        CUME_DIST() OVER (ORDER BY tier_value_dense_rank DESC) AS tier_value_dense_rank_cume_dist
    FROM reconciled
),
enriched AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY tier_value_dense_rank_cume_dist DESC NULLS LAST) AS tier_value_dense_rank_cume_dist_dense_rank,
        DENSE_RANK() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_dense_rank_2,
        RANK() OVER (ORDER BY tier_value_rank) AS tier_value_rank_rank,
        RANK() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_rank_2,
        DENSE_RANK() OVER (ORDER BY tier_value_cume_dist_percent_rank) AS tier_value_cume_dist_percent_rank_dense_rank
    FROM with_windows
),
cleaned AS (
    SELECT
        id,
        tier_value,
        tier_value_cume_dist,
        tier_value_dense_rank,
        tier_value_cume_dist_percent_rank,
        tier_value_rank_rank,
        tier_value_rank_2,
        tier_value_cume_dist_percent_rank_dense_rank,
        ROUND(tier_value_dense_rank_2 * 0.9, 2) AS adjusted_tier_value_dense_rank_2
    FROM enriched
    WHERE tier_value_cume_dist >= 0.68
)
SELECT
    id,
    tier_value,
    tier_value_cume_dist,
    tier_value_cume_dist_percent_rank,
    tier_value_rank_rank,
    adjusted_tier_value_dense_rank_2
FROM cleaned

WITH flattened AS (
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
),
base AS (
    SELECT *, DATE_FORMAT(effective_from, 'yyyy-MM-dd') AS effective_from_label
    FROM flattened
    WHERE effective_from > TO_DATE('2024-02-19')
),
with_windows AS (
    SELECT
        id,
        effective_from,
        RANK() OVER w_recent AS tier_value_rank,
        CUME_DIST() OVER (ORDER BY tier_value NULLS FIRST) AS tier_value_cume_dist,
        MIN(tier_value) OVER (PARTITION BY currency) AS currency_min_tier_value,
        CUME_DIST() OVER w_hist AS tier_value_cume_dist_2
    FROM base
    WINDOW w_recent AS (PARTITION BY currency ORDER BY tier_value DESC), w_hist AS (ORDER BY tier_value)
),
metrics AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY tier_value_cume_dist_2) AS tier_value_cume_dist_2_dense_rank,
        RANK() OVER (ORDER BY tier_value_rank) AS tier_value_rank_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value_cume_dist_2 DESC NULLS LAST) AS tier_value_cume_dist_2_percent_rank
    FROM with_windows
)
SELECT
    id,
    effective_from,
    tier_value_rank,
    currency_min_tier_value,
    tier_value_rank_rank,
    tier_value_cume_dist_2_percent_rank,
    RANK() OVER w_ordered AS tier_value_cume_dist_2_rank,
    DENSE_RANK() OVER (ORDER BY tier_value_rank DESC NULLS LAST) AS tier_value_rank_dense_rank,
    PERCENT_RANK() OVER (ORDER BY tier_value_cume_dist_2_dense_rank DESC NULLS FIRST) AS tier_value_cume_dist_2_dense_rank_percent_rank
FROM metrics
WHERE tier_value_rank_rank > 4
WINDOW w_ordered AS (ORDER BY tier_value_cume_dist_2 NULLS LAST);

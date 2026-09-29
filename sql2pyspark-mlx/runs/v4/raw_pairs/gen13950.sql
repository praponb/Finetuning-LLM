WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
)
-- Running and moving aggregates
SELECT
    *,
    CUME_DIST() OVER w_recent AS tier_value_cume_dist,
    RANK() OVER (ORDER BY tier_value DESC) AS tier_value_rank,
    PERCENT_RANK() OVER (ORDER BY tier_value) AS tier_value_percent_rank
FROM lines
WINDOW w_recent AS (ORDER BY tier_value DESC NULLS LAST);

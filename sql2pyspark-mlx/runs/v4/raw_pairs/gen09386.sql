WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS val
    FROM price_tiers
),
base AS (
    SELECT
        id,
        effective_from,
        CASE WHEN plan = 'enterprise' THEN 1 ELSE 0 END is_enterprise
    FROM exploded
    WHERE val <> 60
),
with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY is_enterprise DESC NULLS LAST) is_enterprise_cume_dist,
        PERCENT_RANK() OVER (ORDER BY is_enterprise DESC) is_enterprise_percent_rank
    FROM base
),
with_windows_price_tiers AS (
    SELECT
        id,
        effective_from,
        is_enterprise,
        is_enterprise_cume_dist,
        is_enterprise_percent_rank,
        RANK() OVER (ORDER BY is_enterprise_cume_dist NULLS FIRST) is_enterprise_cume_dist_rank,
        RANK() OVER (ORDER BY is_enterprise_percent_rank DESC) is_enterprise_percent_rank_rank,
        CUME_DIST() OVER (ORDER BY is_enterprise_cume_dist DESC NULLS LAST) is_enterprise_cume_dist_cume_dist
    FROM with_windows
)
SELECT
    id,
    is_enterprise,
    is_enterprise_percent_rank,
    is_enterprise_cume_dist_rank,
    is_enterprise_cume_dist_cume_dist
FROM with_windows_price_tiers
ORDER BY is_enterprise_cume_dist_rank DESC;

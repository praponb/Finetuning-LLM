WITH flattened AS (
    SELECT
        id,
        effective_from,
        EXPLODE(tier_prices) AS val
    FROM price_tiers
),
cleaned AS (
    SELECT *, DATE_FORMAT(effective_from, 'MMM yyyy') AS effective_from_label
    FROM flattened
    WHERE effective_from > DATE '2024-04-29'
),
filtered AS (
    SELECT *, DATE_TRUNC('MONTH', effective_from) AS effective_from_period
    FROM cleaned
    WHERE val >= 59
),
with_windows AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY val DESC NULLS FIRST) AS val_dense_rank,
        PERCENT_RANK() OVER (ORDER BY val DESC NULLS FIRST) AS val_percent_rank,
        RANK() OVER (ORDER BY val NULLS FIRST) AS val_rank
    FROM filtered
),
with_windows_price_tiers AS (
    SELECT
        *,
        MAX(val_rank) OVER (ORDER BY CAST(effective_from_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS val_rank_max_30d,
        RANK() OVER (ORDER BY val_dense_rank NULLS FIRST) AS val_dense_rank_rank
    FROM with_windows
)
SELECT
    id,
    effective_from_label,
    effective_from_period,
    val_percent_rank,
    val_rank,
    val_dense_rank_rank
FROM with_windows_price_tiers
ORDER BY val_dense_rank_rank

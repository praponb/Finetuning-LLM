WITH lines AS (
    SELECT
        id,
        effective_from,
        EXPLODE_OUTER(discounts) AS val
    FROM price_tiers
),
with_windows AS (
    SELECT
        *,
        RANK() OVER (ORDER BY val NULLS LAST) AS val_rank,
        PERCENT_RANK() OVER (ORDER BY val DESC NULLS FIRST) AS val_percent_rank,
        RANK() OVER (ORDER BY val NULLS LAST) AS val_rank_2
    FROM lines
),
remaining_keys AS (
    SELECT id
    FROM with_windows
    EXCEPT ALL
    SELECT id
    FROM price_tiers
    WHERE effective_from <= '2023-10-01'
),
kept_rows AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.id = k.id
),
base AS (
    SELECT *, val_rank_2 - val AS val_rank_2_minus_val
    FROM kept_rows
    WHERE effective_from >= DATE '2024-10-29'
      AND val BETWEEN 45 AND 77
),
report AS (
    SELECT
        id,
        effective_from,
        val,
        val_rank_2,
        val_rank_2_minus_val
    FROM base
)
SELECT
    id,
    effective_from,
    val,
    val_rank_2_minus_val
FROM report
ORDER BY val NULLS FIRST

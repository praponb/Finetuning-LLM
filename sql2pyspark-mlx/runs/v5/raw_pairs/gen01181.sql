WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) x AS val
),
filtered AS (
    SELECT *, CEIL(val) AS val_whole
    FROM lines
    WHERE plan <> 'enterprise'
),
cleaned AS (
    SELECT
        id,
        plan,
        effective_from,
        val,
        val_whole,
        val_whole - val AS val_whole_minus_val
    FROM filtered
    WHERE effective_from > TO_DATE('2024-06-10')
      AND plan IN ('enterprise', 'basic')
),
kept_keys AS (
    SELECT id, plan
    FROM cleaned
    EXCEPT
    SELECT id, plan
    FROM price_tiers
    WHERE MONTH(effective_from) IN (1, 10, 12)
),
kept_rows AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.id = k.id AND s.plan = k.plan
),
valid AS (
    SELECT plan, DATE_TRUNC('YEAR', effective_from) AS effective_from_period
    FROM kept_rows
    WHERE effective_from BETWEEN '2023-07-11' AND '2025-07-01'
),
totals AS (
    SELECT
        plan,
        DATE_TRUNC('QUARTER', effective_from_period) AS quarter_start,
        (UNIX_TIMESTAMP(MAX(effective_from_period)) - UNIX_TIMESTAMP(MIN(effective_from_period))) / 60 AS span_minutes,
        COUNT(*) AS cnt
    FROM valid
    GROUP BY plan, DATE_TRUNC('QUARTER', effective_from_period)
)
SELECT
    plan,
    quarter_start,
    span_minutes,
    cnt
FROM totals;

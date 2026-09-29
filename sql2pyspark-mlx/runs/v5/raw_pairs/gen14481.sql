WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(cost * 0.85, 2) AS cost_adjusted,
        COALESCE(cost, 0) AS cost_filled
    FROM shipments
    WHERE carrier IS NOT NULL
      AND status = 'RETURNED'
)
-- Keep one row per key (latest first)
SELECT
    id,
    order_id,
    carrier,
    shipped_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost_filled NULLS FIRST, id) AS rank_in_group
    FROM valid
) x
WHERE rank_in_group = 1;

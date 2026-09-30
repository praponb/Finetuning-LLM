WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) x AS val
),
base AS (
    SELECT *, DATE_FORMAT(effective_from, 'yyyy-MM-dd') effective_from_label
    FROM flattened
    WHERE effective_from < '2024-12-01'
),
base_price_tiers AS (
    SELECT
        *,
        COALESCE(val, 0) val_filled,
        ROUND(val, 1) val_rounded
    FROM base
    WHERE effective_from > DATE '2024-03-06'
),
filtered AS (
    SELECT *, CASE WHEN plan IN ('pro', 'enterprise') THEN 'Core' ELSE 'Other' END plan_group
    FROM base_price_tiers
    WHERE effective_from > DATE '2024-05-29'
),
enriched AS (
    SELECT
        *,
        COUNT(val_filled) OVER w_part plan_count_val_filled,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY val_rounded NULLS LAST) val_rounded_cume_dist,
        DENSE_RANK() OVER (ORDER BY val DESC) val_dense_rank,
        RANK() OVER (ORDER BY val_rounded DESC) val_rounded_rank,
        SUM(val_filled) OVER w_part plan_sum_val_filled
    FROM filtered
    WINDOW w_part AS (PARTITION BY plan)
)
SELECT
    id,
    plan,
    val,
    effective_from_label,
    val_filled,
    plan_count_val_filled,
    val_rounded_cume_dist,
    val_dense_rank,
    plan_sum_val_filled
FROM enriched
ORDER BY plan DESC, plan_sum_val_filled DESC;

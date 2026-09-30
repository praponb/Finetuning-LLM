WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        pos,
        li_item.qty
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) li AS pos, li_item
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        pos,
        qty,
        ROUND(pos * 1.07, 2) AS adjusted_pos,
        CAST(pos AS DOUBLE) AS pos_num
    FROM exploded
    WHERE created_at >= DATE '2025-03-01'
      AND customer_id IS NOT NULL
      AND qty > 2
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY customer_id ORDER BY pos NULLS FIRST) AS pos_rank,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY adjusted_pos DESC) AS adjusted_pos_cume_dist,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY pos DESC NULLS FIRST) AS pos_dense_rank
    FROM filtered
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(pos, 2) AS pos_rounded
    FROM scored
    WHERE customer_id IS NOT NULL
)
SELECT
    id,
    pos,
    qty,
    adjusted_pos,
    pos_rank,
    adjusted_pos_cume_dist,
    pos_dense_rank,
    pos_rounded
FROM base
ORDER BY pos, adjusted_pos DESC;

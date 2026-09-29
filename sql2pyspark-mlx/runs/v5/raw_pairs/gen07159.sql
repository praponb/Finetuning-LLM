WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        CAST(quantity AS DOUBLE) AS quantity_str
    FROM order_items
    WHERE product_id IS NOT NULL
      AND unit_price > 270
),
with_dims AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.quantity_str,
        d.product_name,
        d.brand,
        d.launch_date
    FROM cleaned src
    LEFT JOIN products d
        ON src.product_id <=> d.id
),
enriched AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.quantity_str,
        p.cost
    FROM with_dims e
    LEFT JOIN products p
        ON e.product_id = p.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        quantity_str,
        NTILE(4) OVER (ORDER BY cost, id) AS cost_bucket,
        DENSE_RANK() OVER (ORDER BY cost DESC NULLS LAST) AS cost_dense_rank,
        LAG(cost, 2) OVER (PARTITION BY product_id ORDER BY quantity_str, id) AS prev_cost
    FROM enriched
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_str DESC, id) AS row_num
    FROM scored
    QUALIFY row_num = 1
)
SELECT
    id,
    product_id,
    quantity_str,
    cost_bucket,
    cost_dense_rank,
    prev_cost,
    row_num
FROM ranked
ORDER BY row_num

WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY quantity DESC NULLS LAST, id) AS rn
        FROM order_items
    ) ranked
    WHERE rn = 1
),
with_dims AS (
    -- Attach products attributes
    SELECT src.*, d.stock_qty
    FROM ranked src
    JOIN products d
        ON src.product_id = d.id
),
base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(quantity, 0) quantity_rounded
    FROM with_dims
    WHERE product_id IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        rn,
        stock_qty,
        quantity_rounded,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY quantity_rounded NULLS FIRST) quantity_rounded_dense_rank,
        NTILE(5) OVER (PARTITION BY order_id ORDER BY quantity DESC, id) quantity_bucket
    FROM base
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        order_id,
        BIT_AND(quantity) AS quantity_bit_and,
        APPROX_PERCENTILE(quantity_rounded_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS quantity_rounded_dense_rank_quartiles,
        MAX(quantity_bucket) AS max_quantity_bucket,
        ROUND(AVG(quantity_bucket), 2) AS avg_quantity_bucket,
        COUNT(DISTINCT product_id) AS distinct_product_ids,
        COUNT(DISTINCT id) AS distinct_ids
    FROM scored
    GROUP BY order_id
)
SELECT
    order_id,
    quantity_bit_and,
    max_quantity_bucket,
    avg_quantity_bucket
FROM grouped
ORDER BY max_quantity_bucket DESC;

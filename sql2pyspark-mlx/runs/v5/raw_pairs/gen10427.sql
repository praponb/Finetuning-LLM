WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE last_restock >= DATE '2023-09-05'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        RANK() OVER (PARTITION BY product_id ORDER BY quantity_filled DESC NULLS FIRST) AS quantity_filled_rank,
        NTILE(4) OVER (ORDER BY quantity_filled DESC, id) AS quantity_filled_bucket,
        MIN(quantity_filled) OVER (PARTITION BY product_id) AS product_id_min_quantity_filled
    FROM valid
    WHERE product_id IN (4, 9, 10)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        product_id,
        quantity_filled_bucket
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_filled_bucket DESC, id) AS dedup_rank
        FROM metrics
    ) t
    WHERE dedup_rank = 1
),
joined AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.product_id,
        a.quantity_filled_bucket,
        d.launch_date,
        d.cost,
        d.price
    FROM ranked a
    INNER JOIN products d
        ON a.product_id = d.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        product_id,
        price,
        cost
    FROM joined
) src
UNPIVOT (
    metric_value FOR metric_name IN (price, cost)
);

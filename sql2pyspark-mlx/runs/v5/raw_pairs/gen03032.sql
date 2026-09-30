WITH with_dims AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.quantity,
        e.last_restock,
        p.category,
        p.cost
    FROM inventory e
    JOIN products p
        ON e.product_id = p.id
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT product_id, id
    FROM with_dims
    INTERSECT ALL
    SELECT product_id, id
    FROM inventory
    WHERE quantity <= 122
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    JOIN remaining_keys k ON s.product_id = k.product_id AND s.id = k.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, SUM(quantity) OVER (PARTITION BY category) AS category_sum_quantity
    FROM verified
),
with_dims_inventory AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.product_id,
        a.quantity,
        a.category,
        a.cost,
        p.cost AS product_cost
    FROM with_windows a
    LEFT JOIN products p
        ON a.product_id = p.id
),
with_dims_inventory_2 AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.product_cost,
        p.stock_qty,
        p.category AS product_category
    FROM with_dims_inventory src
    LEFT JOIN products p
        ON src.product_id = p.id
    WHERE p.supplier_id IS NULL
)
SELECT
    id,
    product_id,
    product_cost
FROM with_dims_inventory_2
ORDER BY product_cost DESC

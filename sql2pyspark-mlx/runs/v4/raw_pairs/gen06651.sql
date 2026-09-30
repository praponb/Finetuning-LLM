WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        CASE WHEN quantity >= 267 THEN 'High' WHEN quantity >= 54 THEN 'Medium' ELSE 'Low' END AS quantity_tier
    FROM inventory
    WHERE last_restock >= DATE '2023-08-16'
      AND product_id IN (4, 8)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM valid
    EXCEPT ALL
    SELECT product_id, id
    FROM inventory
    WHERE last_restock BETWEEN '2023-12-01' AND '2024-10-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id AND s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        ROUND(quantity * 0.85, 2) AS quantity_adjusted
    FROM reconciled
    WHERE quantity >= 116
),
enriched AS (
    -- Attach products attributes
    SELECT s.*, p.product_name
    FROM base s
    INNER JOIN products p
        ON s.product_id = p.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        quantity_adjusted,
        INITCAP(TRIM(product_name)) AS product_name_title,
        quantity + quantity_adjusted AS quantity_plus_quantity_adjusted
    FROM enriched
    WHERE product_id IN (4, 5, 7, 9)
),
base_inventory AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, quantity / NULLIF(quantity_adjusted, 0) AS quantity_per_quantity_adjusted
    FROM filtered
    WHERE quantity_adjusted >= 73
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.product_name_title,
    (SELECT AVG(x.quantity) FROM inventory x WHERE x.product_id = s.product_id) AS avg_inventory_quantity
FROM base_inventory s
ORDER BY id NULLS LAST;

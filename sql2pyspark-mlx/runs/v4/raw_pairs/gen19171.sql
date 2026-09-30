WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE last_restock > DATE '2023-10-25'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY last_restock NULLS FIRST, id) = 1
),
joined AS (
    -- Attach products attributes
    SELECT
        s.*,
        d.launch_date,
        d.brand
    FROM deduped s
    LEFT JOIN products d
        ON s.product_id = d.id
),
with_dims AS (
    -- Attach stores attributes
    SELECT src.*, s.store_name
    FROM joined src
    JOIN stores s
        ON src.store_id = s.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, product_id
    FROM with_dims
    EXCEPT ALL
    SELECT id, product_id
    FROM inventory
    WHERE product_id IN (1, 3, 4, 5)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    JOIN kept_keys k ON s.id = k.id AND s.product_id = k.product_id
)
-- Multi-level aggregation
SELECT
    store_id,
    CASE WHEN quantity_filled >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_filled_tier,
    AVG(quantity_filled) FILTER (WHERE brand = 'Acme') AS acme_avg_quantity_filled,
    SIZE(COLLECT_SET(brand)) AS num_brands,
    GROUPING(store_id) AS grouping_id
FROM reconciled
GROUP BY ROLLUP (store_id, CASE WHEN quantity_filled >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY quantity_filled_tier DESC NULLS FIRST;

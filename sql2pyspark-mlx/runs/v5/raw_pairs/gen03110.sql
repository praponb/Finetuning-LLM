WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        CASE WHEN quantity >= 233 THEN 'High' WHEN quantity >= 49 THEN 'Medium' ELSE 'Low' END quantity_band
    FROM inventory
    WHERE last_restock > TO_DATE('2023-03-02')
      AND last_restock < '2023-09-01'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(quantity) quantity_whole,
        CEIL(quantity) quantity_whole_2
    FROM filtered
    WHERE quantity BETWEEN 119 AND 172
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT store_id, id
    FROM valid
    INTERSECT ALL
    SELECT store_id, id
    FROM inventory
    WHERE product_id = 9
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN remaining_keys k ON s.store_id = k.store_id AND s.id = k.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT product_id, store_id
    FROM verified
    INTERSECT ALL
    SELECT product_id, store_id
    FROM inventory
    WHERE product_id IN (1, 5)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    INNER JOIN matched_keys k ON s.product_id = k.product_id AND s.store_id = k.store_id
)
-- Multi-level aggregation
SELECT
    CASE WHEN quantity >= 206 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_tier,
    APPROX_PERCENTILE(quantity_whole_2, ARRAY(0.25, 0.5, 0.75)) AS quantity_whole_2_quartiles,
    MAX(quantity) AS max_quantity
FROM reconciled
GROUP BY 1
ORDER BY quantity_tier DESC;

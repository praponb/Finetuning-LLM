WITH kept_keys AS (
    SELECT store_id, id
    FROM inventory
    INTERSECT ALL
    SELECT store_id, id
    FROM inventory
    WHERE product_id IN (1, 3, 4, 10)
),
reconciled AS (
    SELECT s.*
    FROM inventory s
    JOIN kept_keys k ON s.store_id = k.store_id AND s.id = k.id
)
SELECT
    DATE_TRUNC('QUARTER', last_restock) AS quarter_start,
    product_id,
    CASE WHEN quantity >= 266 THEN 'HIGH' ELSE 'LOW' END AS quantity_tier,
    SUM(quantity) AS total_quantity,
    AVG(quantity) AS avg_quantity,
    COUNT(*) AS row_count,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    COUNT(*) AS event_count,
    COUNT(DISTINCT id) AS distinct_ids
FROM reconciled
GROUP BY 1, 2, 3;

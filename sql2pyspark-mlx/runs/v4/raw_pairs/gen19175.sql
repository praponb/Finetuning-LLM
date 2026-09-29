WITH enriched AS (
    -- Attach products attributes
    SELECT s.*, p.price
    FROM inventory s
    LEFT JOIN products p
        ON s.product_id = p.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY price, id) AS event_seq,
        NTILE(10) OVER (PARTITION BY product_id ORDER BY price DESC, id) AS price_bucket,
        LEAD(price, 1) OVER (ORDER BY last_restock, id) AS next_price,
        AVG(quantity) OVER (PARTITION BY store_id, product_id) AS store_id_avg_quantity
    FROM enriched
),
remaining_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM scored
    EXCEPT
    SELECT product_id, id
    FROM inventory
    WHERE quantity BETWEEN 104 AND 211
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN remaining_keys k ON s.product_id = k.product_id AND s.id = k.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', last_restock) AS activity_month,
    CASE WHEN quantity >= 192 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    MIN(next_price) AS min_next_price,
    MIN(price) AS min_price
FROM reconciled
GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', last_restock), CASE WHEN quantity >= 192 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('MONTH', last_restock)), ())

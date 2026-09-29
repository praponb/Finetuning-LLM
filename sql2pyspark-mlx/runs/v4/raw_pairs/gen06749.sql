WITH remaining_keys AS (
    -- Set difference on keys
    SELECT supplier_id, id
    FROM products
    EXCEPT
    SELECT supplier_id, id
    FROM products
    WHERE supplier_id IN (1, 4)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM products s
    INNER JOIN remaining_keys k ON s.supplier_id = k.supplier_id AND s.id = k.id
)
-- Grouped metrics
SELECT
    brand,
    DATE_TRUNC('MONTH', launch_date) AS activity_month,
    COUNT(*) AS row_count,
    AVG(stock_qty) AS avg_stock_qty,
    BIT_OR(stock_qty) AS stock_qty_bit_or,
    SIZE(COLLECT_SET(brand)) AS num_brands,
    COUNT(*) FILTER (WHERE brand = 'Umbrella') AS umbrella_rows,
    MAX(cost) FILTER (WHERE brand = 'Stark') AS stark_max_cost
FROM reconciled
GROUP BY brand, DATE_TRUNC('MONTH', launch_date);

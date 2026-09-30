WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM store_sales
    INTERSECT ALL
    SELECT id
    FROM store_sales
    WHERE sale_date < '2024-11-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM store_sales s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT
    store_id,
    sale_date,
    units,
    revenue
FROM reconciled
ORDER BY revenue DESC;

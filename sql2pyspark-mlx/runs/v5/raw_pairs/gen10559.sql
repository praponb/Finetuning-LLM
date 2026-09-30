WITH remaining_keys AS (
    -- Set difference on keys
    SELECT product_id
    FROM store_sales
    EXCEPT DISTINCT
    SELECT product_id
    FROM store_sales
    WHERE revenue BETWEEN 2600 AND 2900
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM store_sales s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM verified
GROUP BY DATE_TRUNC('QUARTER', sale_date)
ORDER BY distinct_ids_2 DESC

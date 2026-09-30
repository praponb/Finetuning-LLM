WITH matched_keys AS (
    SELECT supplier_id, category
    FROM products
    EXCEPT ALL
    SELECT supplier_id, category
    FROM products
    WHERE YEAR(launch_date) >= 2023
),
verified AS (
    SELECT s.*
    FROM products s
    INNER JOIN matched_keys k ON s.supplier_id = k.supplier_id AND s.category = k.category
)
SELECT
    supplier_id,
    category,
    ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN brand = 'Globex' THEN 1 END), 0), 2) AS cost_per_globex,
    COUNT(DISTINCT category) AS distinct_categories,
    STDDEV_SAMP(stock_qty) AS stock_qty_stddev,
    COUNT(CASE WHEN brand = 'Initech' THEN 1 END) AS initech_count
FROM verified
GROUP BY GROUPING SETS ((supplier_id, category), (supplier_id))
HAVING cost_per_globex < 0;

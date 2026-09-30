WITH filtered AS (
    SELECT
        *,
        INITCAP(TRIM(category)) AS category_title,
        UPPER(product_name) AS product_name_upper
    FROM products
    WHERE supplier_id IS NOT NULL
)
SELECT
    dim.city,
    a.category,
    COUNT(*) AS rows_total,
    SUM(a.cost) AS total_cost
FROM filtered a
INNER JOIN suppliers dim ON a.supplier_id = dim.id
GROUP BY GROUPING SETS ((dim.city, a.category), (dim.city), ())
ORDER BY category DESC NULLS LAST;

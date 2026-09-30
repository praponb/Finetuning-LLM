WITH filtered AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        INITCAP(TRIM(product_name)) product_name_title
    FROM products
    WHERE launch_date > TO_DATE('2023-08-08')
      AND brand IS NOT NULL
)
SELECT
    d.city,
    src.category,
    COUNT(*) AS rows_total,
    MIN(src.price) AS min_price,
    MAX(src.cost) AS max_cost,
    GROUPING_ID() AS gid
FROM filtered src
INNER JOIN suppliers d ON src.supplier_id = d.id
GROUP BY ROLLUP (d.city, src.category)

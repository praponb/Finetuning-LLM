WITH by_dimension AS (
    SELECT
        dim.category,
        COUNT(*) AS rows_total,
        MAX(src.quantity) AS max_quantity
    FROM inventory src
    JOIN products dim ON src.product_id = dim.id
    GROUP BY dim.category
)
SELECT
    category,
    rows_total,
    max_quantity
FROM by_dimension;

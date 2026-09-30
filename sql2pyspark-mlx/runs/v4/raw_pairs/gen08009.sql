WITH cleaned AS (
    SELECT
        id,
        product_name,
        category,
        price,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        ARRAY_CONTAINS(tags, 'clearance') AS is_clearance,
        COALESCE(brand, 'UNKNOWN') AS brand_filled
    FROM products
    WHERE launch_date > DATE '2023-08-11'
)
SELECT *
FROM (
    SELECT
        id,
        category,
        price,
        cost
    FROM cleaned
) src
UNPIVOT (
    metric_value FOR measure IN (price, cost AS COST_VAL)
);

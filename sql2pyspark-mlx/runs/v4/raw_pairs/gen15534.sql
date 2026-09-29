WITH cleaned AS (
    SELECT
        id,
        category,
        brand,
        price,
        cost,
        launch_date,
        tags,
        supplier_id,
        stock_qty * cost AS stock_qty_times_cost,
        COALESCE(brand, 'DIRECT') AS brand_clean
    FROM products
    WHERE cost <= 310
)
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY launch_date DESC NULLS FIRST, id) AS rank_in_group
    FROM cleaned
) x
WHERE rank_in_group = 1;

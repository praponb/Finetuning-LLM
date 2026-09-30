WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        price,
        stock_qty,
        launch_date,
        supplier_id,
        REPLACE(product_name, ' ', '') AS product_name_slug
    FROM products
    WHERE launch_date >= DATE '2023-08-24'
      AND supplier_id IS NOT NULL
)
-- QUALIFY-style window filter
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY price, id) AS latest_rank
    FROM filtered
) t
WHERE latest_rank = 1

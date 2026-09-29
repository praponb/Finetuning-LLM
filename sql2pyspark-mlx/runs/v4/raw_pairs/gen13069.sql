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
        tags,
        supplier_id,
        LAST_DAY(launch_date) AS launch_date_month_end,
        ROUND(price * 0.9, 2) AS price_adjusted,
        COALESCE(brand, 'OTHER') AS brand_filled
    FROM products
    WHERE brand IS NOT NULL
      AND ARRAY_CONTAINS(tags, 'new')
)
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand, category ORDER BY cost, id) AS row_num
    FROM filtered
) x
WHERE row_num = 1;

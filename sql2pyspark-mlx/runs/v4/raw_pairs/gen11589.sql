SELECT *
FROM (
    SELECT
        brand,
        supplier_id,
        category,
        price
    FROM products
) src
PIVOT (
    SUM(price) AS amount,
    COUNT(1) AS item_count
    FOR category IN ('Fashion', 'Books')
)

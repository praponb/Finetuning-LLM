SELECT *
FROM (
    SELECT
        id,
        supplier_id,
        cost,
        price
    FROM products
) src
UNPIVOT (
    reading FOR metric IN (cost, price)
)

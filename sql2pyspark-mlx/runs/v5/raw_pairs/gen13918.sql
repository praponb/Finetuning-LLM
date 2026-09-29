SELECT *
FROM (
    SELECT
        id,
        category,
        price,
        cost
    FROM products
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric IN (price, cost)
) unp

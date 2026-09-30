SELECT *
FROM (
    SELECT
        id,
        brand,
        cost,
        price
    FROM products
) src
UNPIVOT (
    value FOR metric IN (cost, price)
);

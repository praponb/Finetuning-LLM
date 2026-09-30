SELECT *
FROM (
    SELECT
        id,
        brand,
        price,
        cost
    FROM products
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR kpi IN (price AS PRICE_VAL, cost)
);

SELECT *
FROM (
    SELECT
        category,
        brand,
        stock_qty
    FROM products
) src
PIVOT (
    MAX(stock_qty) AS value_sum,
    COUNT(1) AS item_count
    FOR brand IN ('Initech', 'Stark')
);

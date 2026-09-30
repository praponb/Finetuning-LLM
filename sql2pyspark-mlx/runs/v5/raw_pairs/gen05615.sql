WITH latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY cost DESC, id) AS row_num
        FROM products
    ) x
    WHERE row_num = 1
)
SELECT
    category,
    BIT_OR(stock_qty) AS stock_qty_bit_or,
    MAX(price) FILTER (WHERE brand = 'Stark') AS stark_max_price
FROM latest
GROUP BY 1
HAVING BIT_OR(stock_qty) < 1000
ORDER BY stark_max_price DESC NULLS FIRST

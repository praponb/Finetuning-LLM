WITH numbered AS (
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY shipping_fee) AS rn
    FROM orders
)
SELECT
    id,
    status,
    shipping_fee,
    discount
FROM numbered
WHERE rn <= 3
ORDER BY 4 DESC;

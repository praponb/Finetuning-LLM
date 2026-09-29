WITH Ranked AS (
    SELECT
        id,
        channel,
        amount,
        discount,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount, id) row_num
    FROM orders
)
SELECT
    id,
    channel,
    amount,
    discount
FROM Ranked
WHERE row_num = 1
ORDER BY channel;

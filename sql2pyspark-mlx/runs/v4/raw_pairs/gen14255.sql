WITH TopOrders AS (
    SELECT
        id,
        status,
        amount,
        customer_id,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS rnk
    FROM orders
)
SELECT *
FROM TopOrders
WHERE rnk <= 2;

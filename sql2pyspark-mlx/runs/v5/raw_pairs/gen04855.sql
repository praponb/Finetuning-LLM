WITH RankedPayments AS (
    SELECT
        id,
        status,
        amount,
        order_id,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS rn
    FROM payments
)
SELECT
    id,
    status,
    amount,
    order_id
FROM RankedPayments
WHERE rn <= 1
ORDER BY status, id DESC

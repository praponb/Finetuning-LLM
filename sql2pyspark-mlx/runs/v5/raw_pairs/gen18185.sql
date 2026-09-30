SELECT *
FROM (
    SELECT
        order_id,
        status,
        method,
        amount
    FROM payments
) src
PIVOT (
    MAX(amount)
    FOR method IN ('promptpay', 'cash')
)

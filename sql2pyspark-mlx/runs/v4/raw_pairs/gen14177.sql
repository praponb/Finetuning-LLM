SELECT *
FROM (
    SELECT
        store_id,
        channel,
        amount
    FROM orders
) src
PIVOT (
    MAX(amount) AS amount,
    COUNT(1) AS cnt
    FOR channel IN ('store', 'partner')
)

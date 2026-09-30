SELECT *
FROM (
    SELECT
        status,
        channel,
        discount
    FROM orders
) src
PIVOT (
    MAX(discount)
    FOR channel IN ('web' AS web, 'mobile' AS mobile)
)

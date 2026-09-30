SELECT *
FROM (
    SELECT
        order_id,
        carrier,
        cost
    FROM shipments
) src
PIVOT (
    AVG(cost)
    FOR carrier IN ('FedEx' AS fedex, 'Flash' AS flash)
)

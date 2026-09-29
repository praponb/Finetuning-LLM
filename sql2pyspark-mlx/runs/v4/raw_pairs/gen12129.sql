SELECT *
FROM (
    SELECT
        order_id,
        carrier,
        cost
    FROM shipments
) src
PIVOT (
    SUM(cost) AS total,
    COUNT(1) AS cnt
    FOR carrier IN ('FedEx' AS fedex, 'Kerry' AS kerry, 'DHL' AS dhl)
)

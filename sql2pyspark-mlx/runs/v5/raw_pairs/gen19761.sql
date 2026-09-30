SELECT *
FROM (
    SELECT
        order_id,
        carrier,
        cost
    FROM shipments
) src
PIVOT (
    SUM(cost) AS amount,
    COUNT(1) AS item_count
    FOR carrier IN ('DHL', 'Kerry')
)

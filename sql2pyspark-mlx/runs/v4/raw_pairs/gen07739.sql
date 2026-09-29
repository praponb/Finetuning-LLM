SELECT *
FROM (
    SELECT
        carrier,
        status,
        cost
    FROM shipments
) src
PIVOT (
    SUM(cost)
    FOR status IN ('RETURNED' AS returned, 'IN_TRANSIT' AS in_transit, 'DELIVERED' AS delivered)
)

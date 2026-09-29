SELECT *
FROM (
    SELECT
        carrier,
        status,
        cost
    FROM shipments
) src
PIVOT (
    SUM(cost) AS value_sum,
    COUNT(1) AS item_count
    FOR status IN ('IN_TRANSIT' AS in_transit, 'LOST' AS lost, 'RETURNED' AS returned)
)

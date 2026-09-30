SELECT *
FROM (
    SELECT
        carrier,
        status,
        cost
    FROM shipments
) src
PIVOT (
    AVG(cost) AS amount,
    COUNT(1) AS item_count
    FOR status IN ('IN_TRANSIT' AS in_transit, 'DELIVERED' AS delivered)
);

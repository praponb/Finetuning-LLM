SELECT *
FROM (
    SELECT
        carrier,
        order_id,
        status,
        cost
    FROM shipments
) src
PIVOT (
    SUM(cost) AS total
    FOR status IN ('RETURNED', 'IN_TRANSIT')
);

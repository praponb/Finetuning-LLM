SELECT *
FROM (
    SELECT status, AVG(cost) AS avg_cost, COUNT(DISTINCT cost) AS unique_carriers
    FROM shipments
    GROUP BY status
) AS sub
WHERE avg_cost >= 42;

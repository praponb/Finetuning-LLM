SELECT carrier, max_cost
FROM (
    SELECT carrier, MAX(cost) AS max_cost
    FROM shipments
    WHERE cost IS NOT NULL
    GROUP BY carrier
) AS agg
WHERE max_cost > 28
ORDER BY max_cost DESC, carrier
LIMIT 20;

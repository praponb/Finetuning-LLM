SELECT carrier, cost_count, first_delivered_date
FROM (
    SELECT carrier, COUNT(cost) AS cost_count, MIN(delivered_date) AS first_delivered_date
    FROM shipments
    GROUP BY carrier
) AS x
WHERE cost_count > 5
ORDER BY first_delivered_date DESC, carrier

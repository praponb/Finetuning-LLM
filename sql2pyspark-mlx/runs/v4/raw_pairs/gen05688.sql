SELECT carrier, returned_count
FROM (
    SELECT carrier, COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count
    FROM shipments
    GROUP BY carrier
) AS agg
WHERE returned_count > 3
ORDER BY returned_count DESC;

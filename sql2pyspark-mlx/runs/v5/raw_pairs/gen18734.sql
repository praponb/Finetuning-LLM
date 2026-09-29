SELECT *
FROM (
    SELECT status, MIN(amount) AS min_amount
    FROM orders
    GROUP BY status
) AS agg
WHERE min_amount > 1270
ORDER BY status ASC

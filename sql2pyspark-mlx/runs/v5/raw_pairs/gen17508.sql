SELECT COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count, MIN(cost) AS min_cost, MIN(delivered_date) AS first_delivered_date
FROM shipments
ORDER BY first_delivered_date ASC;

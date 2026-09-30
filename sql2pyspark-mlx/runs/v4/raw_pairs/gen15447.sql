SELECT shipped_month, latest_delivered_date, avg_cost
FROM (
    SELECT MONTH(shipped_date) AS shipped_month, MAX(delivered_date) AS latest_delivered_date, ROUND(AVG(cost), 2) AS avg_cost
    FROM shipments
    GROUP BY MONTH(shipped_date)
) AS agg
WHERE avg_cost > 60
ORDER BY latest_delivered_date DESC

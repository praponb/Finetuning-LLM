WITH Src AS (
    SELECT *, LAST_DAY(delivered_date) AS delivered_date_month_end
    FROM shipments
    WHERE YEAR(shipped_date) >= 2024
),
Summary AS (
    SELECT status, ROUND(STDDEV(cost), 2) AS stddev_cost, MAX(cost) AS max_cost
    FROM Src
    GROUP BY status
)
SELECT status, stddev_cost, max_cost
FROM Summary
WHERE max_cost > 77
ORDER BY max_cost

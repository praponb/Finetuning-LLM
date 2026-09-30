SELECT *
FROM (
    SELECT YEAR(shipped_date) AS shipped_year, COUNT(cost) AS cost_count, SUM(CASE WHEN carrier = 'FedEx' THEN cost ELSE 0 END) AS fedex_cost
    FROM shipments
    GROUP BY YEAR(shipped_date)
) AS x
WHERE cost_count < 6
ORDER BY fedex_cost DESC;

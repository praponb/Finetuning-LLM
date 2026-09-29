SELECT delivered_year, in_transit_cost, stddev_cost
FROM (
    SELECT YEAR(delivered_date) AS delivered_year, SUM(CASE WHEN status = 'IN_TRANSIT' THEN cost ELSE 0 END) AS in_transit_cost, ROUND(STDDEV(cost), 2) AS stddev_cost
    FROM shipments
    WHERE YEAR(shipped_date) >= 2023
    GROUP BY YEAR(delivered_date)
) AS sub
WHERE in_transit_cost < 32

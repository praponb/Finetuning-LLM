SELECT *
FROM (
    SELECT
        QUARTER(delivered_date) AS delivered_quarter,
        CASE WHEN cost >= 86 THEN 'high' WHEN cost >= 38 THEN 'mid' ELSE 'low' END AS cost_bucket,
        MIN(shipped_date) AS first_shipped_date,
        ROUND(STDDEV(cost), 2) AS stddev_cost
    FROM shipments
    GROUP BY QUARTER(delivered_date), CASE WHEN cost >= 86 THEN 'high' WHEN cost >= 38 THEN 'mid' ELSE 'low' END
) AS s
WHERE stddev_cost > 64;

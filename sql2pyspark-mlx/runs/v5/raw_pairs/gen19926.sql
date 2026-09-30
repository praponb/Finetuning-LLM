SELECT
    MONTH(launch_date) AS launch_month,
    ROUND((100 * SUM(CASE WHEN brand = 'Initech' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_initech,
    MAX(cost) - MIN(cost) AS cost_range,
    ROUND((100 * SUM(CASE WHEN category = 'Sports' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_sports
FROM products
WHERE MONTH(launch_date) IN (1, 2, 4) AND (supplier_id IS NULL OR launch_date >= '2025-02-01')
GROUP BY MONTH(launch_date)
ORDER BY 2, launch_month
LIMIT 5;

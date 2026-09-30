WITH recent_products AS (
    SELECT *, DATE_TRUNC('week', launch_date) AS launch_date_period
    FROM products
    WHERE supplier_id IN (1, 7, 38)
),
agg AS (
    SELECT MONTH(launch_date) AS launch_month, MAX(cost) - MIN(cost) AS cost_range, MIN(launch_date) AS min_launch_date
    FROM recent_products
    GROUP BY MONTH(launch_date)
)
SELECT *
FROM agg
WHERE cost_range < 21;

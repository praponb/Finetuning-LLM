WITH src AS (
    SELECT *
    FROM products
    WHERE supplier_id IN (1, 29, 31, 39)
),
launch_month_summary AS (
    SELECT DATE_TRUNC('MONTH', launch_date) AS launch_month, MAX(launch_date) AS last_launch_date
    FROM src
    GROUP BY DATE_TRUNC('MONTH', launch_date)
)
SELECT launch_month, last_launch_date
FROM launch_month_summary;

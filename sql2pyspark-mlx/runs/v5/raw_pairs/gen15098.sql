SELECT *
FROM (
    SELECT
        supplier_id,
        DATE_FORMAT(launch_date, 'yyyy-MM') launch_month,
        COUNT(price) price_count,
        SUM(cost) total_cost
    FROM products
    WHERE category = 'Fashion'
    GROUP BY supplier_id, DATE_FORMAT(launch_date, 'yyyy-MM')
) x
WHERE price_count > 8

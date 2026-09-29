SELECT * FROM (SELECT DATE_TRUNC('MONTH', launch_date) AS launch_month, brand, cost FROM products WHERE category IN ('Grocery', 'Electronics', 'Home'))
PIVOT (MAX(cost) FOR brand IN ('Stark', 'Globex'))
ORDER BY launch_month

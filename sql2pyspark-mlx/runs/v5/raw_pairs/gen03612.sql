WITH ProductsBySupplier AS (
    SELECT supplier_id, MAX(launch_date) AS latest_launch_date
    FROM products
    WHERE stock_qty BETWEEN 104 AND 274
    GROUP BY supplier_id
)
SELECT s.id, s.country, P.latest_launch_date
FROM suppliers s
LEFT JOIN ProductsBySupplier P ON s.id = P.supplier_id
ORDER BY P.latest_launch_date, s.country

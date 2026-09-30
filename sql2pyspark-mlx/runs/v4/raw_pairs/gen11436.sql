WITH Src AS (
    SELECT *
    FROM orders
    WHERE channel <> 'web'
),
summary AS (
    SELECT YEAR(order_date) AS order_year, COUNT(discount) AS discount_count
    FROM Src
    GROUP BY YEAR(order_date)
)
SELECT order_year, discount_count
FROM summary
WHERE discount_count > 5
ORDER BY discount_count DESC;

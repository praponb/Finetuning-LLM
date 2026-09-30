SELECT *
FROM (
    SELECT YEAR(order_date) AS order_year, COUNT(discount) AS discount_count
    FROM orders
    WHERE store_id = 4
    GROUP BY YEAR(order_date)
) AS agg
WHERE discount_count < 5
ORDER BY discount_count DESC, order_year
LIMIT 10

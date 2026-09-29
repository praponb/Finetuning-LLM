SELECT revenue
FROM store_sales
WHERE sale_date BETWEEN '2023-08-01' AND '2024-07-28'
UNION ALL
SELECT amount
FROM payments
ORDER BY revenue DESC;

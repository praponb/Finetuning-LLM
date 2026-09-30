SELECT CASE WHEN method IN ('promptpay', 'paypal') THEN 'priority' ELSE 'other' END AS method_group, SUM(amount) AS total_amount, SUM(SUM(amount)) OVER (ORDER BY CASE WHEN method IN ('promptpay', 'paypal') THEN 'priority' ELSE 'other' END) AS cumulative_amount
FROM payments
WHERE status != 'SUCCESS'
GROUP BY CASE WHEN method IN ('promptpay', 'paypal') THEN 'priority' ELSE 'other' END
ORDER BY 2 DESC, method_group
LIMIT 20

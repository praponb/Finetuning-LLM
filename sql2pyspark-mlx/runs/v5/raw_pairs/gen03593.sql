SELECT status, method, SUM(CASE WHEN method = 'promptpay' THEN amount ELSE 0 END) AS promptpay_amount
FROM payments
WHERE order_id IS NULL AND amount > 650 AND TO_DATE(paid_at) < '2023-05-02'
GROUP BY status, method
HAVING SUM(CASE WHEN method = 'promptpay' THEN amount ELSE 0 END) > 56
ORDER BY 3, method;

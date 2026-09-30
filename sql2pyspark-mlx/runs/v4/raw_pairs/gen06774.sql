SELECT id, amount, channel
FROM orders
WHERE customer_id IN (15, 23) AND (discount IS NULL OR order_date BETWEEN '2024-03-01' AND '2025-06-02')
ORDER BY channel;

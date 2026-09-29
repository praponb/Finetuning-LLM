SELECT id, method, amount, SUM(amount) OVER (PARTITION BY method) AS method_sum_amount FROM payments;

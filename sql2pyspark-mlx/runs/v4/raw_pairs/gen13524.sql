SELECT id, status, amount, RANK() OVER (PARTITION BY status ORDER BY amount) rnk FROM payments WHERE TO_DATE(paid_at) >= '2024-06-01';

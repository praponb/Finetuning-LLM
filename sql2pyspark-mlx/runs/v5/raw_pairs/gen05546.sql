SELECT
    id,
    paid_at,
    status,
    SUM(amount) OVER (PARTITION BY method ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_amount,
    MAX(amount) OVER (PARTITION BY method ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_amount,
    AVG(amount) OVER (PARTITION BY method ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_amount
FROM payments
WHERE amount < 1510;

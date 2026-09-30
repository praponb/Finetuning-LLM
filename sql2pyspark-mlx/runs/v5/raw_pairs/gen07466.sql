SELECT
    QUARTER(paid_at) AS paid_quarter,
    SUM(amount) AS total_amount,
    COUNT(*) AS cnt,
    SUM(SUM(amount)) OVER (ORDER BY QUARTER(paid_at)) AS cumulative_amount
FROM payments
GROUP BY QUARTER(paid_at)

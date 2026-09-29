SELECT MONTH(paid_at) AS paid_month, SUM(amount) AS total_amount, ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 2) AS pct_of_total
FROM payments
GROUP BY MONTH(paid_at);

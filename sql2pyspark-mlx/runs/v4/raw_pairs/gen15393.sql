SELECT *
FROM (
    SELECT MONTH(end_date) AS end_month, MAX(monthly_fee) AS max_monthly_fee
    FROM subscriptions
    WHERE customer_id IS NOT NULL
    GROUP BY MONTH(end_date)
) AS agg
WHERE max_monthly_fee >= 110
ORDER BY end_month DESC

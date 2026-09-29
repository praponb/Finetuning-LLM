WITH filtered_subscriptions AS (
    SELECT *, DATE_FORMAT(start_date, 'yyyy-MM-dd') AS start_date_label
    FROM subscriptions
    WHERE monthly_fee >= 180
),
subscriptions_stats AS (
    SELECT DATE_FORMAT(end_date, 'yyyy-MM') AS end_month, SUM(monthly_fee) AS total_monthly_fee, COUNT(monthly_fee) AS monthly_fee_count
    FROM filtered_subscriptions
    GROUP BY DATE_FORMAT(end_date, 'yyyy-MM')
)
SELECT end_month, total_monthly_fee, monthly_fee_count
FROM subscriptions_stats
ORDER BY end_month

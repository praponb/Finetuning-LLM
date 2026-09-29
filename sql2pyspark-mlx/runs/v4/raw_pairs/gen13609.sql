SELECT
    id,
    end_date,
    status,
    monthly_fee,
    LAST_DAY(end_date) AS end_date_month_end
FROM subscriptions
WHERE status = 'PAUSED'
ORDER BY end_date, id
LIMIT 3

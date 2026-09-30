SELECT start_date, end_date, CAST(monthly_fee AS INT) AS monthly_fee_int FROM subscriptions WHERE status = 'EXPIRED' AND MONTH(end_date) IN (7, 8, 11) ORDER BY 2 DESC

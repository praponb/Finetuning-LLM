SELECT id, start_date, monthly_fee, COALESCE(monthly_fee, 0) AS monthly_fee_filled FROM subscriptions WHERE customer_id = 29 AND start_date <= '2024-07-01' ORDER BY 2 ASC

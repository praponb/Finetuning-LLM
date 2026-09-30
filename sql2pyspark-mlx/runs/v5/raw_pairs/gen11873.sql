SELECT plan, DAYOFWEEK(start_date) AS start_date_dayofweek, FLOOR(monthly_fee) AS monthly_fee_whole FROM subscriptions WHERE customer_id IN (26, 33, 36) AND plan = 'standard' ORDER BY plan;

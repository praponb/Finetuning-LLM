select status, customer_id, max(monthly_fee) - min(monthly_fee) as monthly_fee_range from subscriptions group by status, customer_id;

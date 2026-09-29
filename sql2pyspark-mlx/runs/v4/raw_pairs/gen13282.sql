select customer_id, count(monthly_fee) as monthly_fee_count from subscriptions group by customer_id

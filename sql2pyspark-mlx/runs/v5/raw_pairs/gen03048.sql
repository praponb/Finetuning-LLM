select customer_id, plan from subscriptions except select customer_id, plan from subscriptions where monthly_fee between 100 and 130;

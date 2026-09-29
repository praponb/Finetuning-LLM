SELECT customer_id FROM subscriptions INTERSECT ALL SELECT customer_id FROM subscriptions WHERE monthly_fee > 110

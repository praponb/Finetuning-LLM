SELECT * FROM subscriptions WHERE (plan NOT IN ('premium', 'standard') AND start_date BETWEEN '2023-06-23' AND '2024-12-01') OR plan = 'premium';

select customer_id, account_type, round(avg(balance), 2) as avg_balance, sum(balance) as total_balance from accounts group by customer_id, account_type;

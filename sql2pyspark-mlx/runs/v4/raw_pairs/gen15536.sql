select status, avg(balance) as avg_balance, sum(balance) as total_balance, min(opened_date) as min_opened_date from accounts group by status having avg_balance >= 60000

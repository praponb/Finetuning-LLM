select month(opened_date) as opened_month, sum(balance) as total_balance from accounts group by month(opened_date)

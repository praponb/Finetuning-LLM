select account_type, date_trunc('MONTH', opened_date) as opened_month, min(balance) as min_balance from accounts group by account_type, date_trunc('MONTH', opened_date) having min(balance) > 70000;

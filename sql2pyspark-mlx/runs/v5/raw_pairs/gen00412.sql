select date_trunc('MONTH', opened_date) as opened_month, count(distinct balance) as unique_account_types from accounts group by date_trunc('MONTH', opened_date)

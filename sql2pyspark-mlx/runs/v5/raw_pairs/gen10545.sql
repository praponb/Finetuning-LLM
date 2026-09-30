select opened_date, status, balance, last_day(opened_date) as opened_date_month_end, upper(account_type) as account_type_upper from accounts where account_type = 'SAVINGS'

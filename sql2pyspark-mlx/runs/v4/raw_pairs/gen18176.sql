select id, account_type, opened_date, lag(opened_date, 1) over (partition by account_type order by opened_date, id) as prev_opened_date from accounts

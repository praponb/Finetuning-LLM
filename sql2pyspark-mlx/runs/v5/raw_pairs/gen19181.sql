select id, opened_date, lead(opened_date, 1) over (order by opened_date, id) as next_opened_date from accounts where account_type in ('CREDIT', 'LOAN', 'SAVINGS') order by next_opened_date asc

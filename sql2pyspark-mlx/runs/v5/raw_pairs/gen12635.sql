select id, opened_date, percent_rank() over (order by opened_date) as opened_date_pct_rank from accounts

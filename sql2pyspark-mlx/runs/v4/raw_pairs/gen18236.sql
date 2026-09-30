select id, status, opened_date, percent_rank() over (partition by status order by opened_date) as opened_date_pct_rank from accounts where status in ('FROZEN', 'CLOSED');

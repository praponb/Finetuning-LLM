select id, status, balance, percent_rank() over (partition by status order by balance desc) as balance_pct_rank from accounts where year(opened_date) = 2025

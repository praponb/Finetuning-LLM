select
    id,
    status,
    opened_date,
    balance,
    percent_rank() over (partition by status order by opened_date) as opened_date_pct_rank,
    percent_rank() over (partition by status order by balance) as balance_pct_rank
from accounts
where account_type in ('LOAN', 'SAVINGS', 'CHECKING');

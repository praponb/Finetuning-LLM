select
    status,
    date_trunc('week', opened_date) as week_start,
    round(sum(balance) / nullif(count(case when status = 'CLOSED' then 1 end), 0), 2) as balance_per_closed,
    avg(balance) filter (where account_type = 'SAVINGS') as savings_avg_balance,
    count(*) filter (where account_type = 'CHECKING') as checking_rows,
    approx_percentile(balance, array(0.25, 0.5, 0.75)) as balance_quartiles,
    approx_percentile(balance, 0.95) as balance_p95,
    grouping_id() as grouping_level
from accounts
group by grouping sets ((status, date_trunc('week', opened_date)), (status), ())
having savings_avg_balance < 0
order by balance_p95 desc;

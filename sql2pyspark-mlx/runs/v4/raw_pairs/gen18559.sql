select
    status,
    method,
    count(distinct amount) as unique_statuss,
    round((100 * sum(case when method = 'cash' then 1 else 0 end)) / count(*), 1) as pct_cash
from payments
where paid_at < '2025-06-01' and amount < 540
group by status, method
order by unique_statuss desc, method, status
limit 50;

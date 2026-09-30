select
    c.is_active,
    date_trunc('MONTH', c.signup_date) as signup_month,
    round((100 * sum(case when a.status = 'OPEN' then 1 else 0 end)) / count(*), 1) as pct_open,
    sum(case when a.status = 'FROZEN' then 1 else 0 end) as frozen_count
from accounts a
inner join customers c on a.customer_id = c.id
group by c.is_active, date_trunc('MONTH', c.signup_date);

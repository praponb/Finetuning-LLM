select
    d.city,
    a.account_type,
    count(*) as rows_total,
    max(a.balance) as max_balance,
    grouping_id() as grouping_level
from accounts a
inner join customers d on a.customer_id = d.id
group by rollup (d.city, a.account_type)
order by max_balance desc;

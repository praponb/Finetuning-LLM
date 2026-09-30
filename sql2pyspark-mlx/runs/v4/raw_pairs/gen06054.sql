select
    a.id,
    a.status,
    a.balance,
    t.cnt
from accounts a
join (select account_id, count(*) as cnt
    from transactions
    group by account_id) t on a.id = t.account_id
order by a.balance asc, id desc

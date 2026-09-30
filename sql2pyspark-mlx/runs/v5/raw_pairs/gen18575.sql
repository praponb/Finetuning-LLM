select
    a.status,
    a.balance,
    t.is_flagged,
    t.txn_time,
    t.merchant,
    substring(a.status, 1, 2) as status_prefix
from accounts a
inner join transactions t on t.account_id = a.id
where t.merchant in ('Grab', 'Amazon', 'Starbucks') or a.status in ('FROZEN', 'CLOSED', 'OPEN')
order by 4

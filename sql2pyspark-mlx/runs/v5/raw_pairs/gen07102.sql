select
    t.id,
    t.merchant,
    t.txn_type,
    a.account_type,
    a.balance,
    case when a.account_type = 'LOAN' then 1 else 0 end as is_loan
from transactions t
full outer join accounts a on a.id = t.account_id
where t.amount >= 4000 and t.merchant = 'Grab'
order by 1 desc

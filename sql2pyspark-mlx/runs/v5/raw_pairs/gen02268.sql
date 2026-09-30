with account_summary as (
    select account_id, min(txn_time) as min_txn_time, sum(case when txn_type = 'FEE' then 1 else 0 end) as fee_count
    from transactions
    where merchant not in ('Lazada', 'Starbucks', 'Grab')
    group by account_id
)
select
    accounts.id,
    accounts.status,
    account_summary.min_txn_time,
    account_summary.fee_count
from accounts
left outer join account_summary on accounts.id = account_summary.account_id
where account_summary.fee_count > 69
order by id desc, fee_count asc

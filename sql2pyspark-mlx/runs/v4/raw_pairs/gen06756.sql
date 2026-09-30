with transactions_by_account as (
    select account_id, count(case when txn_type = 'DEBIT' then 1 end) as debit_count, max(amount) - min(amount) as amount_range
    from transactions
    group by account_id
)
select
    a.id,
    a.customer_id,
    a.balance,
    tba.debit_count,
    tba.amount_range
from accounts a
inner join transactions_by_account tba on a.id = tba.account_id
order by balance desc, customer_id desc

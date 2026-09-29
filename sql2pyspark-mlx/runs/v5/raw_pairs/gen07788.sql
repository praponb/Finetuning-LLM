with transactions_by_account as (
    select account_id, count(amount) as amount_count, count(*) as row_count
    from transactions
    where amount between 2200 and 3100
    group by account_id
)
select
    a.id,
    a.customer_id,
    a.account_type,
    tba.amount_count,
    tba.row_count
from accounts a
left outer join transactions_by_account tba on a.id = tba.account_id
where tba.row_count >= 7
order by a.account_type desc

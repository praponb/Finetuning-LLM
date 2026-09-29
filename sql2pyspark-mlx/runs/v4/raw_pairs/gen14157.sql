with transactions_by_account as (
    select account_id, sum(case when merchant = 'Amazon' then 1 else 0 end) as amazon_count
    from transactions
    group by account_id
)
select a.id, a.balance, tba.amazon_count
from accounts a
left join transactions_by_account tba on a.id = tba.account_id
order by balance;

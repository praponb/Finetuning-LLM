with AccountStats as (
    select account_id, round(stddev(amount), 2) as stddev_amount, min(amount) as min_amount
    from transactions
    group by account_id
)
select accounts.balance, AccountStats.stddev_amount, AccountStats.min_amount
from accounts
left outer join AccountStats on accounts.id = AccountStats.account_id;

with account_stats as (
    select account_id, max(amount) - min(amount) as amount_range
    from transactions
    where amount is not null
    group by account_id
)
select accounts.status, accounts.customer_id, account_stats.amount_range
from accounts
left join account_stats on accounts.id = account_stats.account_id

select accounts.id, count(transactions.id) as transaction_count
from accounts
join transactions on transactions.account_id = accounts.id
where accounts.opened_date < '2024-07-01'
group by accounts.id;

select a.balance, t.max_amount
from accounts a
inner join (select account_id, max(amount) as max_amount
    from transactions
    group by account_id) t on a.id = t.account_id
where t.max_amount > 2900;

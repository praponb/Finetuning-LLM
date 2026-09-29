select a.opened_date, a.balance, t.total_amount
from accounts a
left join (select account_id, sum(amount) as total_amount
    from transactions
    group by account_id) t on a.id = t.account_id
where t.total_amount >= 19000
order by total_amount desc;

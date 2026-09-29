select a.id, count(t.id) as transaction_count, max(t.amount) - min(t.amount) as amount_range
from accounts a
left join transactions t on t.account_id = a.id
where hour(t.txn_time) between 7 and 15
group by a.id
order by amount_range

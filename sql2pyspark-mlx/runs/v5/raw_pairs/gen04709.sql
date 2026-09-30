select account_id, max(amount) - min(amount) as amount_range, count(amount) as amount_count
from transactions
where amount is not null
group by account_id
having sum(case when txn_type = 'CREDIT' then 1 else 0 end) >= 34
order by amount_range, account_id
limit 5;

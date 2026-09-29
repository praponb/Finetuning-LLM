select a.customer_id, case when a.balance >= 62000 then 'high' when a.balance >= 17000 then 'mid' else 'low' end as balance_bucket, sort_array(collect_set(t.txn_type)) as txn_type_list
from transactions t
inner join accounts a on t.account_id = a.id
inner join customers c on c.id = a.customer_id
where c.credit_limit >= 15200
group by a.customer_id, case when a.balance >= 62000 then 'high' when a.balance >= 17000 then 'mid' else 'low' end

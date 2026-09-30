select
    a.account_type,
    a.status,
    count(t.amount) amount_count,
    sum(case when t.merchant = 'Amazon' then t.amount else 0 end) amazon_amount
from transactions t
inner join accounts a on t.account_id = a.id
right join customers c on c.id = a.customer_id
where t.txn_type not in ('FEE', 'DEBIT', 'REFUND')
group by a.account_type, a.status

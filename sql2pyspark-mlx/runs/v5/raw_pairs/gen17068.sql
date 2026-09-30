select
    a.id,
    a.account_type,
    count(t.id) as transaction_count,
    round(avg(t.amount), 2) as avg_amount,
    sum(case when t.merchant = 'Lazada' then t.amount else 0 end) as lazada_amount
from accounts a
left outer join transactions t on t.account_id = a.id
group by a.id, a.account_type
having avg_amount > 42;

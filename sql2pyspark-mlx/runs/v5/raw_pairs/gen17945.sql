select
    id,
    customer_id,
    balance,
    dense_rank() over (partition by customer_id order by balance desc) as balance_dense_rank
from accounts
where opened_date between '2023-11-03' and '2024-03-01'
order by id desc

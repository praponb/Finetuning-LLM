with top_payments as (
    select *, dense_rank() over (partition by status order by amount) as rnk
    from payments
)
select
    id,
    status,
    amount,
    order_id
from top_payments
where rnk <= 3
order by amount desc

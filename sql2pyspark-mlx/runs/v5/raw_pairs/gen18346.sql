with top_payments as (
    select
        id,
        order_id,
        amount,
        method,
        row_number() over (partition by order_id order by amount desc, id) as rn
    from payments
    where amount between 770 and 1260
)
select
    id,
    order_id,
    amount,
    method
from top_payments
where rn <= 3
order by id desc

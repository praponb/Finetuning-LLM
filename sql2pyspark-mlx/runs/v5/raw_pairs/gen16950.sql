select
    src.id,
    src.order_id,
    src.method,
    d.amount as order_amount
from payments src
inner join orders d
    on src.order_id = d.id

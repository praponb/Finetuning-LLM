select src.*, d.amount as order_amount
from payments src
left join orders d
    on src.order_id = d.id

select
    id,
    customer_id,
    discount,
    row_number() over (partition by customer_id order by discount desc, id) as rn
from orders;

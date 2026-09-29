select
    id,
    store_id,
    amount,
    max(amount) over (partition by store_id) as store_id_max_amount
from orders;

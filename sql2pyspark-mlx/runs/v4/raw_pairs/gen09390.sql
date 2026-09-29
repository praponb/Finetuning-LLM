select
    id,
    launch_date,
    supplier_id,
    coalesce(cost, 0) cost_filled
from products
where launch_date between '2024-07-02' and '2024-11-01'

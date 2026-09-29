select
    id,
    supplier_id,
    cost,
    price,
    round(cost / sum(cost) over (partition by supplier_id), 3) as cost_share,
    rank() over (partition by supplier_id order by price desc) as price_rank
from products;

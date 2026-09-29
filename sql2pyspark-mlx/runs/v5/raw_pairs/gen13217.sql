select
    id,
    brand,
    cost,
    ntile(3) over (partition by brand order by cost, id) as cost_ntile
from products
order by brand asc;

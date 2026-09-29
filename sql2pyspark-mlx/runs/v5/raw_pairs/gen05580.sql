select
    id,
    category,
    launch_date,
    stock_qty,
    cost,
    lead(stock_qty, 1) over (partition by category order by launch_date, id) as next_stock_qty,
    dense_rank() over (order by cost desc) as cost_dense_rank
from products;

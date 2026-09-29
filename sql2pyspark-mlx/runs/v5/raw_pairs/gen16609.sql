select
    id,
    product_name,
    category,
    launch_date,
    tags,
    supplier_id,
    max(price) over (partition by category, supplier_id order by price asc, id asc rows between 6 preceding and current row) as last7_max_price,
    avg(price) over (partition by category, supplier_id order by price asc, id asc rows between 6 preceding and current row) as last7_avg_price,
    sum(price) over (partition by category, supplier_id order by price asc, id asc rows between 6 preceding and current row) as last7_sum_price,
    min(price) over (partition by category, supplier_id order by price asc, id asc rows between 6 preceding and current row) as last7_min_price
from products

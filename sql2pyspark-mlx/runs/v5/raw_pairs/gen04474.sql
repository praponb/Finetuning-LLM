select
    *,
    min(revenue) over (partition by store_id, product_id order by revenue, id rows between unbounded preceding and current row) as running_min_revenue,
    max(revenue) over (partition by store_id, product_id order by revenue, id rows between unbounded preceding and current row) as running_max_revenue,
    avg(revenue) over (partition by store_id, product_id order by revenue, id rows between unbounded preceding and current row) as running_avg_revenue,
    row_number() over (partition by product_id order by units desc, id) as seq_no
from store_sales;

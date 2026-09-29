select
    id,
    store_id,
    sale_date,
    units,
    product_id,
    revenue,
    sum(units) over (partition by store_id order by sale_date, id) as running_units,
    dense_rank() over (partition by product_id order by revenue desc) as revenue_dense_rank
from store_sales
where year(sale_date) >= 2025

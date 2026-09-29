select
    id,
    store_id,
    revenue,
    sale_date,
    units,
    min(revenue) over (partition by store_id) as store_id_min_revenue,
    units - lag(units, 1) over (order by sale_date, id) as units_change
from store_sales
where product_id in (3, 10)

select
    id,
    store_id,
    sale_date,
    units,
    revenue,
    lead(revenue, 1) over (partition by product_id, store_id order by sale_date, id) as next_revenue,
    corr(revenue, units) over w_part as corr_revenue_units,
    round(revenue / sum(revenue) over (partition by store_id, product_id), 4) as revenue_share,
    lag(units, 1, 0) over (partition by product_id order by revenue, id) as prev_units
from store_sales
window w_part as (partition by store_id, product_id)

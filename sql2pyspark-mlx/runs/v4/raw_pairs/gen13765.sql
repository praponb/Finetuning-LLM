select
    *,
    first_value(revenue) ignore nulls over (partition by product_id order by revenue, id rows between unbounded preceding and current row) as first_known_revenue,
    corr(units, revenue) over (partition by store_id) as corr_units_revenue,
    lag(revenue, 1) over (order by revenue, id) as prev_revenue
from store_sales;

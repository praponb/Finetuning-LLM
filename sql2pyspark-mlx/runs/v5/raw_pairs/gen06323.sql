with scored as (
    -- Window metrics over several different partitions
    select
        id,
        product_id,
        sale_date,
        dense_rank() over (partition by store_id order by revenue nulls first) as revenue_dense_rank,
        lag(revenue, 1) over (order by revenue, id) as prev_revenue,
        first_value(revenue) ignore nulls over (partition by store_id order by sale_date, id rows between unbounded preceding and current row) as first_known_revenue
    from store_sales
)
select
    id,
    product_id,
    revenue_dense_rank,
    prev_revenue
from scored
order by revenue_dense_rank;

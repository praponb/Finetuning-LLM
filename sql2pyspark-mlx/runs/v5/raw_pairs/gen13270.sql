with metrics as (
    -- Ranking and gap features
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        first_value(units, true) over (partition by product_id, store_id order by revenue, id rows between unbounded preceding and current row) as first_known_units,
        corr(revenue, units) over (partition by product_id, store_id) as corr_revenue_units,
        last_value(revenue) ignore nulls over (partition by product_id order by revenue, id rows between unbounded preceding and current row) as last_known_revenue,
        avg(revenue) over w_running as revenue_avg_6_prior
    from store_sales
    where sale_date <= '2023-03-01'
    window w_running as (partition by store_id, product_id order by revenue, id rows between 6 preceding and 1 preceding)
)
select
    id,
    product_id,
    units,
    first_known_units,
    corr_revenue_units,
    last_known_revenue,
    revenue_avg_6_prior
from metrics
order by id desc nulls last;

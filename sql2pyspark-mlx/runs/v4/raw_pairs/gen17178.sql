with enriched as (
    select
        *,
        count(revenue) over (partition by product_id) as product_id_count_revenue,
        corr(revenue, units) over (partition by product_id) as corr_revenue_units,
        avg(revenue) over (partition by store_id, product_id order by revenue, id rows between 3 preceding and current row) as revenue_avg_3,
        sum(units) over (partition by product_id, store_id order by revenue, id rows between unbounded preceding and current row) as cumulative_units,
        avg(revenue) over (partition by product_id order by sale_date, id rows between 5 preceding and 1 preceding) as revenue_avg_5_prior
    from store_sales
)
select
    s.id,
    s.store_id,
    s.product_id,
    s.sale_date,
    s.units,
    s.revenue,
    s.product_id_count_revenue,
    s.revenue_avg_3,
    s.cumulative_units,
    (select min(x.revenue) from store_sales x where x.store_id = s.store_id) as min_store_sale_revenue
from enriched s
order by id desc nulls first
limit 20;

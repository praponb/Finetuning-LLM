with ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by product_id order by quantity, id) as dedup_rank
        from order_items
    ) t
    where dedup_rank = 1
)
-- Aggregate with conditional metrics
select
    product_id,
    order_id,
    approx_percentile(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles,
    min(quantity) as min_quantity,
    percentile_approx(unit_price, 0.95) as unit_price_p95
from ranked
group by 1, 2

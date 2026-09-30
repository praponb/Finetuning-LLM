with latest as (
    select *
    from (
        select *, row_number() over (partition by product_id, store_id order by quantity, id) as rn
        from inventory
    ) x
    where rn = 1
)
select
    date_trunc('year', last_restock) as year_start,
    date_trunc('month', last_restock) as activity_month,
    percentile_approx(quantity, 0.95) as quantity_p95,
    round(avg(quantity), 2) as avg_quantity,
    stddev_samp(quantity) as quantity_stddev,
    approx_percentile(quantity, 0.9) as quantity_p90
from latest
group by 1, 2
order by quantity_stddev nulls first;

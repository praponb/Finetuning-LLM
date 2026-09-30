with deduped as (
    select *
    from inventory
    qualify row_number() over (partition by store_id order by last_restock desc, id) = 1
),
filtered as (
    select *, last_day(last_restock) as last_restock_month_end
    from deduped
    where last_restock >= date '2023-03-12'
),
ranked as (
    select *
    from filtered
    qualify rank() over (partition by product_id order by last_restock) = 1
)
select
    product_id,
    count(*) as event_count,
    stddev_samp(quantity) as quantity_stddev,
    round(avg(quantity), 2) as avg_quantity,
    grouping(product_id) as grouping_id
from ranked
group by rollup (product_id)
order by product_id nulls first;

with cleaned as (
    select
        *,
        round(quantity, 0) quantity_rounded,
        last_day(last_restock) last_restock_month_end
    from inventory
    where last_restock > to_date('2024-01-26')
)
select
    date_trunc('quarter', last_restock) as quarter_start,
    case when quantity_rounded >= 63 then 'HIGH' else 'LOW' end as quantity_rounded_tier,
    store_id,
    bit_or(quantity) as quantity_bit_or,
    count(*) as cnt,
    round(avg(quantity_rounded), 2) as avg_quantity_rounded,
    count(distinct store_id) as distinct_store_ids,
    round(avg(quantity), 2) as avg_quantity,
    count(*) as num_rows
from cleaned
group by grouping sets ((date_trunc('quarter', last_restock), case when quantity_rounded >= 63 then 'HIGH' else 'LOW' end, store_id), (date_trunc('quarter', last_restock), case when quantity_rounded >= 63 then 'HIGH' else 'LOW' end), (date_trunc('quarter', last_restock)), ())
order by store_id

with cleaned as (
    -- Keep recent, valid rows
    select *, add_months(last_restock, 3) last_restock_plus_months
    from inventory
    where last_restock >= date '2023-10-17'
      and product_id in (1, 4, 6)
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        last_restock_plus_months,
        last_value(quantity, true) over (order by last_restock, id rows between unbounded preceding and current row) last_known_quantity,
        avg(quantity) over (partition by product_id order by last_restock_plus_months, id rows between 6 preceding and current row) quantity_avg_6
    from cleaned
),
enriched as (
    -- Ranking and gap features
    select
        *,
        ntile(5) over w_grp quantity_bucket,
        rank() over (partition by store_id order by quantity nulls last) quantity_rank,
        ntile(10) over (partition by product_id order by quantity, id) quantity_bucket_2,
        lag(quantity, 1) over (order by last_restock, id) prev_quantity
    from scored
    window w_grp as (order by quantity desc, id)
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from enriched
    qualify row_number() over (partition by store_id order by quantity_bucket_2 desc nulls first, id) = 1
)
-- Multi-level aggregation
select
    date_trunc('quarter', last_restock) as quarter_start,
    case when last_known_quantity >= 243 then 'PREMIUM' else 'STANDARD' end as last_known_quantity_tier,
    max(quantity_avg_6) as max_quantity_avg_6,
    count(*) as event_count,
    grouping(date_trunc('quarter', last_restock)) as gid
from ranked
group by grouping sets ((date_trunc('quarter', last_restock), case when last_known_quantity >= 243 then 'PREMIUM' else 'STANDARD' end), (date_trunc('quarter', last_restock)))

with cleaned as (
    -- Keep recent, valid rows
    select *, round(quantity * 0.85, 2) quantity_adjusted
    from inventory
    where product_id in (3, 4, 5, 10)
),
valid as (
    -- Basic cleansing and defaults
    select *, least(quantity, quantity_adjusted) least_quantity_quantity_adjusted
    from cleaned
    where last_restock >= date '2023-06-08'
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        quantity,
        last_restock,
        quantity_adjusted,
        lag(least_quantity_quantity_adjusted) over w_hist prev_least_quantity_quantity_adjusted,
        lead(quantity_adjusted) over w_hist next_quantity_adjusted,
        lag(quantity_adjusted) over w_hist prev_quantity_adjusted,
        row_number() over w_hist row_seq,
        last_value(quantity, true) over (partition by store_id order by quantity, id rows between unbounded preceding and current row) last_known_quantity
    from valid
    where least_quantity_quantity_adjusted < 48
    window w_hist as (partition by store_id, product_id order by quantity, id)
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        last_restock,
        quantity_adjusted,
        last_known_quantity
    from (
        select *, row_number() over (partition by store_id order by prev_quantity_adjusted nulls last, id) as latest_rank
        from metrics
    ) t
    where latest_rank = 1
),
stats as (
    -- Multi-level aggregation
    select
        date_trunc('year', last_restock) as year_start,
        case when quantity_adjusted >= 82 then 'PREMIUM' else 'STANDARD' end as quantity_adjusted_tier,
        count(*) as cnt,
        round(avg(last_known_quantity), 2) as avg_last_known_quantity,
        sum(quantity_adjusted) as total_quantity_adjusted
    from ranked
    group by cube (date_trunc('year', last_restock), case when quantity_adjusted >= 82 then 'PREMIUM' else 'STANDARD' end)
)
select
    year_start,
    quantity_adjusted_tier,
    cnt,
    avg_last_known_quantity,
    total_quantity_adjusted
from stats
order by cnt desc nulls first;

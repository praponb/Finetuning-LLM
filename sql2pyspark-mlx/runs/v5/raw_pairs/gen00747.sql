with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        product_id,
        last_restock,
        round(quantity * 0.9, 2) as quantity_adjusted
    from inventory
    where last_restock >= date '2023-03-16'
),
enriched as (
    -- Ranking and gap features
    select
        id,
        store_id,
        last_value(quantity_adjusted, true) over (partition by product_id order by last_restock, id rows between unbounded preceding and current row) as last_known_quantity_adjusted,
        avg(quantity_adjusted) over (partition by store_id, product_id) as store_id_avg_quantity_adjusted,
        lag(quantity_adjusted, 1) over (partition by store_id order by last_restock, id) as prev_quantity_adjusted
    from valid
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        last_known_quantity_adjusted,
        least(store_id_avg_quantity_adjusted, prev_quantity_adjusted) as least_store_id_avg_quantity_adjusted_prev_quantity_adjusted,
        coalesce(store_id_avg_quantity_adjusted, 0) as store_id_avg_quantity_adjusted_filled
    from enriched
    where store_id_avg_quantity_adjusted = 76
),
remaining_keys as (
    -- Set intersection on keys
    select store_id, id
    from filtered
    intersect
    select store_id, id
    from inventory
    where product_id = 3
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    join remaining_keys k on s.store_id = k.store_id and s.id = k.id
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        store_id_avg_quantity_adjusted_filled - least_store_id_avg_quantity_adjusted_prev_quantity_adjusted as store_id_avg_quantity_adjusted_filled_minus_least_store_id_avg_quantity_adjusted_prev_quantity_adjusted,
        round(last_known_quantity_adjusted, 1) as last_known_quantity_adjusted_rounded
    from reconciled
    where store_id_avg_quantity_adjusted_filled between 56 and 61
)
-- Grouped metrics
select
    store_id,
    case when store_id_avg_quantity_adjusted_filled_minus_least_store_id_avg_quantity_adjusted_prev_quantity_adjusted >= 81 then 'LARGE' else 'SMALL' end as store_id_avg_quantity_adjusted_filled_minus_least_store_id_avg_quantity_adjusted_prev_quantity_adjusted_tier,
    count(*) as num_rows,
    count(distinct id) as distinct_ids
from cleaned
group by store_id, case when store_id_avg_quantity_adjusted_filled_minus_least_store_id_avg_quantity_adjusted_prev_quantity_adjusted >= 81 then 'LARGE' else 'SMALL' end
order by store_id nulls last;

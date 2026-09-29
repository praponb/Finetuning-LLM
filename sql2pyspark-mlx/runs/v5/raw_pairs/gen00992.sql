with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        quantity,
        last_restock,
        day(last_restock) as last_restock_day,
        round(quantity * 1.07, 2) as quantity_scaled
    from inventory
    where last_restock > date '2023-07-04'
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        min(quantity) over (partition by store_id order by last_restock_day, id rows between 6 preceding and current row) as last7_min_quantity,
        avg(quantity) over (partition by store_id order by last_restock_day, id rows between 6 preceding and current row) as last7_avg_quantity,
        max(quantity) over (partition by store_id order by last_restock_day, id rows between 6 preceding and current row) as last7_max_quantity
    from cleaned
),
cleaned_inventory as (
    -- Filter the raw rows and derive helper columns
    select *, last7_avg_quantity * last_restock_day as last7_avg_quantity_times_last_restock_day
    from with_windows
    where last_restock_day <= 33
),
valid as (
    -- Keep recent, valid rows
    select *, floor(last7_min_quantity) as last7_min_quantity_whole
    from cleaned_inventory
    where last_restock > to_date('2023-02-12')
),
with_dims as (
    -- Attach stores attributes
    select
        s.id,
        s.store_id,
        s.last_restock_day,
        s.last7_min_quantity,
        s.last7_min_quantity_whole,
        b.store_name,
        b.sqft,
        b.city
    from valid s
    left join stores b
        on s.store_id = b.id
),
kept_keys as (
    -- Set intersection on keys
    select id, store_id
    from with_dims
    intersect
    select id, store_id
    from inventory
    where quantity between 80 and 170
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_dims s
    inner join kept_keys k on s.id = k.id and s.store_id = k.store_id
)
-- Aggregate with conditional metrics
select
    city,
    approx_percentile(last_restock_day, 0.95) as last_restock_day_p95,
    count_if(city = 'New York') as new_york_events,
    max(last_restock_day) as max_last_restock_day,
    approx_percentile(last7_min_quantity_whole, array(0.25, 0.5, 0.75)) as last7_min_quantity_whole_quartiles
from reconciled
group by city
order by last_restock_day_p95 desc nulls last;

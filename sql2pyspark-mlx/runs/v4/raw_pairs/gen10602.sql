with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        last_restock,
        round(quantity, 2) as quantity_rounded
    from inventory
    where last_restock > date '2023-04-17'
      and store_id in (1, 4, 5, 6)
),
cleaned_inventory as (
    -- Filter the raw rows and derive helper columns
    select *, ceil(quantity_rounded) as quantity_rounded_whole
    from cleaned
    where last_restock < '2024-12-01'
),
cleaned_inventory_2 as (
    -- Basic cleansing and defaults
    select *, floor(quantity_rounded_whole) as quantity_rounded_whole_whole
    from cleaned_inventory
    where last_restock >= date '2024-04-19'
),
enriched as (
    -- Attach stores attributes
    select
        s.*,
        p2.sqft,
        p2.city,
        p2.opened_date
    from cleaned_inventory_2 s
    left join stores p2
        on s.store_id = p2.id
),
matched_keys as (
    -- Set difference on keys
    select id, store_id
    from enriched
    except
    select id, store_id
    from inventory
    where store_id in (1, 2, 3, 4)
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join matched_keys k on s.id = k.id and s.store_id = k.store_id
)
-- Multi-level aggregation
select
    store_id,
    date_trunc('month', opened_date) as activity_month,
    sort_array(collect_set(city)) as city_list,
    sum(sqft) filter (where city = 'Singapore') as singapore_total_sqft,
    count(*) filter (where city = 'Tokyo') as tokyo_rows,
    sort_array(collect_set(city)) as city_list_2,
    count(*) filter (where city = 'New York') as new_york_rows
from kept_rows
group by grouping sets ((store_id, date_trunc('month', opened_date)), (store_id));

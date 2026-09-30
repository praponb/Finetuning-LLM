with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        quantity,
        last_restock,
        case when quantity >= 263 then 'High' when quantity >= 71 then 'Medium' else 'Low' end as quantity_level
    from inventory
    where last_restock >= date '2023-02-21'
      and last_restock <= '2024-08-07'
),
scored as (
    -- Ranking and gap features
    select
        *,
        count(quantity) over (partition by store_id order by quantity, id rows between unbounded preceding and current row) as running_count_quantity,
        avg(quantity) over (partition by store_id order by quantity, id rows between unbounded preceding and current row) as running_avg_quantity,
        max(quantity) over (partition by store_id order by quantity, id rows between unbounded preceding and current row) as running_max_quantity,
        first_value(quantity, true) over (partition by store_id order by quantity, id rows between unbounded preceding and current row) as first_known_quantity
    from base
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        quantity,
        quantity_level,
        running_avg_quantity,
        running_max_quantity,
        avg(running_count_quantity) over (partition by store_id order by running_avg_quantity, id rows between unbounded preceding and current row) as running_avg_running_count_quantity,
        max(running_count_quantity) over (partition by store_id order by running_avg_quantity, id rows between unbounded preceding and current row) as running_max_running_count_quantity,
        count(running_count_quantity) over (partition by store_id order by running_avg_quantity, id rows between unbounded preceding and current row) as running_count_running_count_quantity,
        sum(running_count_quantity) over (partition by store_id order by running_avg_quantity, id rows between unbounded preceding and current row) as running_sum_running_count_quantity,
        ntile(4) over (partition by store_id order by first_known_quantity, id) as first_known_quantity_bucket
    from scored
),
kept_keys as (
    -- Set difference on keys
    select store_id, id
    from with_windows
    except distinct
    select store_id, id
    from inventory
    where last_restock between '2024-01-30' and '2024-02-16'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    join kept_keys k on s.store_id = k.store_id and s.id = k.id
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        running_avg_quantity,
        running_avg_running_count_quantity,
        running_max_running_count_quantity,
        running_count_running_count_quantity,
        round(running_avg_quantity / running_count_running_count_quantity, 2) as running_avg_quantity_per_running_count_running_count_quantity
    from reconciled
    where running_max_quantity < 130
)
select
    id,
    running_avg_quantity,
    running_max_running_count_quantity,
    running_count_running_count_quantity
from filtered

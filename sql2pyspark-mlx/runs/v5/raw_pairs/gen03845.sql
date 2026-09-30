with valid as (
    -- Keep recent, valid rows
    select
        *,
        ceil(quantity) as quantity_whole,
        cast(quantity as string) as quantity_num
    from inventory
    where last_restock >= to_date('2023-03-15')
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        quantity_whole,
        quantity_num,
        sum(quantity) over (partition by product_id order by last_restock, id rows between unbounded preceding and current row) as running_quantity,
        avg(quantity_whole) over (partition by store_id order by quantity_whole, id rows between 2 preceding and current row) as quantity_whole_avg_2
    from valid
    where quantity_whole != 37
),
with_windows_inventory as (
    -- Running and moving aggregates
    select
        *,
        min(quantity_whole_avg_2) over (order by quantity_whole_avg_2, id rows between unbounded preceding and current row) as running_min_quantity_whole_avg_2,
        max(quantity_whole_avg_2) over (order by quantity_whole_avg_2, id rows between unbounded preceding and current row) as running_max_quantity_whole_avg_2,
        sum(quantity_whole_avg_2) over (order by quantity_whole_avg_2, id rows between unbounded preceding and current row) as running_sum_quantity_whole_avg_2,
        avg(quantity_whole_avg_2) over (order by quantity_whole_avg_2, id rows between unbounded preceding and current row) as running_avg_quantity_whole_avg_2,
        first_value(quantity_whole_avg_2) ignore nulls over (order by quantity_whole, id rows between unbounded preceding and current row) as first_known_quantity_whole_avg_2
    from with_windows
),
kept_keys as (
    -- Set difference on keys
    select id
    from with_windows_inventory
    except
    select id
    from inventory
    where store_id in (1, 2, 4, 5)
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows_inventory s
    inner join kept_keys k on s.id = k.id
),
grouped as (
    -- Multi-level aggregation
    select
        case when running_avg_quantity_whole_avg_2 >= 86 then 'HIGH' else 'LOW' end as running_avg_quantity_whole_avg_2_tier,
        min(running_max_quantity_whole_avg_2) as min_running_max_quantity_whole_avg_2,
        percentile_approx(running_quantity, 0.9) as running_quantity_p90
    from kept_rows
    group by 1
    having running_quantity_p90 < 1000000
)
select running_avg_quantity_whole_avg_2_tier
from grouped
order by running_avg_quantity_whole_avg_2_tier desc nulls last;

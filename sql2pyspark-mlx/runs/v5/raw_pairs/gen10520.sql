with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_id,
        quantity,
        last_restock,
        ceil(quantity) as quantity_whole
    from inventory
    where last_restock > date '2023-07-18'
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        quantity,
        last_restock,
        quantity_whole,
        quantity / nullif(quantity_whole, 0) as quantity_per_quantity_whole
    from valid
    where last_restock > to_date('2023-09-27')
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        quantity,
        last_restock,
        quantity_per_quantity_whole,
        dense_rank() over (order by quantity_whole desc) as quantity_whole_dense_rank,
        row_number() over (order by last_restock desc, id) as seq_no
    from filtered
    where quantity_whole between 24 and 27
),
remaining_keys as (
    -- Set difference on keys
    select id
    from metrics
    except
    select id
    from inventory
    where month(last_restock) in (4, 5, 12)
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    inner join remaining_keys k on s.id = k.id
)
-- Running and moving aggregates
select
    *,
    dense_rank() over (order by seq_no desc nulls last) as seq_no_dense_rank,
    percent_rank() over w_grp as quantity_whole_dense_rank_percent_rank,
    rank() over (order by quantity_per_quantity_whole nulls first) as quantity_per_quantity_whole_rank
from kept_rows
window w_grp as (order by quantity_whole_dense_rank);

with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        coalesce(quantity, 0) as quantity_filled,
        round(quantity * 1.07, 2) as adjusted_quantity
    from inventory
    where store_id in (4, 5)
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        product_id,
        quantity_filled,
        adjusted_quantity,
        avg(adjusted_quantity) over w_running as adjusted_quantity_avg_5,
        dense_rank() over w_recent as adjusted_quantity_dense_rank,
        last_value(quantity, true) over w_hist as last_known_quantity,
        lag(quantity, 2) over w_recent_2 as prev_quantity
    from valid
    window w_running as (partition by store_id order by quantity_filled, id rows between 5 preceding and current row), w_recent as (partition by store_id, product_id order by adjusted_quantity nulls first), w_hist as (partition by store_id, product_id order by quantity, id rows between unbounded preceding and current row), w_recent_2 as (partition by product_id order by adjusted_quantity, id)
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        lead(adjusted_quantity_dense_rank) over w_running as next_adjusted_quantity_dense_rank,
        lag(adjusted_quantity_dense_rank) over w_running as prev_adjusted_quantity_dense_rank,
        lead(adjusted_quantity_avg_5) over w_running as next_adjusted_quantity_avg_5,
        lag(adjusted_quantity_avg_5) over w_running as prev_adjusted_quantity_avg_5,
        row_number() over w_running as event_seq
    from enriched
    window w_running as (partition by product_id order by adjusted_quantity, id)
),
with_dims as (
    -- Attach products attributes
    select e.*, p.cost
    from metrics e
    left join products p
        on e.product_id = p.id
    where array_contains(p.tags, 'premium')
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        sum(quantity_filled) over (partition by store_id order by adjusted_quantity_avg_5, id rows between unbounded preceding and current row) as cumulative_quantity_filled,
        avg(adjusted_quantity) over (partition by store_id order by next_adjusted_quantity_avg_5, id rows between 2 preceding and 1 preceding) as adjusted_quantity_avg_2_prior,
        sum(event_seq) over (partition by store_id order by adjusted_quantity, id rows between unbounded preceding and current row) as cumulative_event_seq
    from with_dims
),
remaining_keys as (
    -- Set intersection on keys
    select product_id
    from with_windows
    intersect all
    select product_id
    from inventory
    where quantity is null
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join remaining_keys k on s.product_id = k.product_id
),
cleaned as (
    -- Keep recent, valid rows
    select
        store_id,
        product_id,
        adjusted_quantity_avg_5,
        adjusted_quantity_dense_rank,
        last_known_quantity,
        next_adjusted_quantity_dense_rank,
        prev_adjusted_quantity_dense_rank,
        next_adjusted_quantity_avg_5,
        prev_adjusted_quantity_avg_5,
        cost,
        cumulative_quantity_filled,
        cumulative_event_seq,
        cast(adjusted_quantity_avg_2_prior as string) as adjusted_quantity_avg_2_prior_num
    from reconciled
    where prev_adjusted_quantity_avg_5 < 58
)
select
    s.product_id,
    s.adjusted_quantity_avg_5,
    s.adjusted_quantity_dense_rank,
    s.last_known_quantity,
    s.next_adjusted_quantity_dense_rank,
    s.prev_adjusted_quantity_dense_rank,
    s.next_adjusted_quantity_avg_5,
    s.cost,
    s.cumulative_quantity_filled,
    (select sum(x.quantity) from inventory x where x.product_id = s.product_id) as sum_inventory_quantity
from cleaned s;

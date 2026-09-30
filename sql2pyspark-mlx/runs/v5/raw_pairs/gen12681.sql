with base as (
    select
        id,
        store_id,
        product_id,
        last_restock,
        ceil(quantity) as quantity_whole,
        coalesce(quantity, 0) as quantity_filled
    from inventory
    where last_restock >= date '2023-05-21'
      and product_id in (6, 7, 8, 9)
),
base_inventory as (
    select
        id,
        store_id,
        last_restock,
        quantity_whole,
        quantity_filled,
        quantity_whole / nullif(quantity_filled, 0) as quantity_whole_per_quantity_filled,
        date_format(last_restock, 'yyyy-MM') as last_restock_label
    from base
    where quantity_whole <= 50
),
latest as (
    select *, rank() over (partition by store_id order by quantity_filled desc nulls first) as latest_rank
    from base_inventory
    qualify latest_rank = 1
),
cleaned as (
    select
        id,
        store_id,
        last_restock,
        quantity_filled,
        quantity_whole_per_quantity_filled,
        last_restock_label,
        latest_rank,
        least(quantity_filled, quantity_whole_per_quantity_filled) as least_quantity_filled_quantity_whole_per_quantity_filled
    from latest
    where last_restock > to_date('2023-03-31')
      and quantity_whole_per_quantity_filled >= 53
),
base_inventory_2 as (
    select *, ceil(latest_rank) as latest_rank_whole
    from cleaned
    where quantity_whole_per_quantity_filled > 76
),
metrics as (
    select
        *,
        dense_rank() over (partition by store_id order by latest_rank_whole desc nulls last) as latest_rank_whole_dense_rank,
        avg(quantity_filled) over (order by latest_rank, id rows between 2 preceding and 1 preceding) as quantity_filled_avg_2_prior,
        lead(quantity_filled, 2) over (partition by store_id order by latest_rank_whole, id) as next_quantity_filled,
        percent_rank() over (partition by store_id order by quantity_whole_per_quantity_filled) as quantity_whole_per_quantity_filled_percent_rank
    from base_inventory_2
    where store_id in (1, 2, 5, 6)
),
scored as (
    select
        id,
        quantity_filled,
        quantity_whole_per_quantity_filled,
        latest_rank_whole,
        next_quantity_filled,
        quantity_whole_per_quantity_filled_percent_rank,
        avg(latest_rank_whole) over (partition by store_id order by last_restock, id rows between 6 preceding and current row) as latest_rank_whole_avg_6,
        last_value(latest_rank_whole) ignore nulls over (partition by store_id order by next_quantity_filled, id rows between unbounded preceding and current row) as last_known_latest_rank_whole,
        row_number() over (order by least_quantity_filled_quantity_whole_per_quantity_filled, id) as seq_no,
        avg(next_quantity_filled) over (partition by store_id) as store_id_avg_next_quantity_filled,
        lag(next_quantity_filled, 2) over (partition by store_id order by last_restock, id) as prev_next_quantity_filled
    from metrics
)
select
    id,
    quantity_filled,
    quantity_whole_per_quantity_filled,
    latest_rank_whole,
    quantity_whole_per_quantity_filled_percent_rank,
    latest_rank_whole_avg_6,
    last_known_latest_rank_whole,
    store_id_avg_next_quantity_filled
from scored
order by quantity_filled, store_id_avg_next_quantity_filled;

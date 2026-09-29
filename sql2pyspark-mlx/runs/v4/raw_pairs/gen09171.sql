with filtered as (
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        coalesce(quantity, 0) as quantity_filled
    from inventory
    where last_restock > to_date('2024-04-02')
),
valid as (
    select
        *,
        last_day(last_restock) as last_restock_month_end,
        quantity * quantity_filled as quantity_times_quantity_filled
    from filtered
    where last_restock >= date '2023-05-15'
),
base as (
    select *, cast(quantity_times_quantity_filled as int) as quantity_times_quantity_filled_int
    from valid
    where last_restock > date '2023-04-03'
),
metrics as (
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        quantity_filled,
        last_restock_month_end,
        quantity_times_quantity_filled,
        quantity_times_quantity_filled_int,
        lead(quantity_times_quantity_filled_int, 1) over (partition by store_id order by quantity asc, id asc) as next_quantity_times_quantity_filled_int,
        dense_rank() over (partition by product_id, store_id order by quantity_times_quantity_filled_int desc) as quantity_times_quantity_filled_int_dense_rank,
        ntile(5) over (partition by store_id, product_id order by quantity desc, id asc) as quantity_bucket,
        cume_dist() over (partition by product_id order by quantity asc) as quantity_cume_dist,
        ntile(5) over (partition by product_id order by quantity asc, id asc) as quantity_bucket_2
    from base
),
with_windows as (
    select
        *,
        dense_rank() over (partition by product_id order by quantity_filled desc nulls first) as quantity_filled_dense_rank,
        sum(quantity) over (partition by store_id order by next_quantity_times_quantity_filled_int asc, id asc rows between unbounded preceding and current row) as running_quantity,
        row_number() over (partition by store_id order by quantity_times_quantity_filled_int_dense_rank desc, id asc) as seq_no,
        first_value(quantity_times_quantity_filled_int_dense_rank, true) over (partition by product_id order by quantity_bucket asc, id asc rows between unbounded preceding and current row) as first_known_quantity_times_quantity_filled_int_dense_rank,
        avg(quantity_bucket) over (order by quantity_times_quantity_filled asc, id asc rows between 5 preceding and 1 preceding) as quantity_bucket_avg_5_prior
    from metrics
)
select
    s.id,
    s.product_id,
    s.quantity,
    s.last_restock,
    s.last_restock_month_end,
    s.quantity_bucket,
    s.running_quantity,
    s.seq_no,
    s.quantity_bucket_avg_5_prior,
    (select min(t2.quantity) from order_items t2 where t2.product_id = s.product_id) as min_order_item_quantity
from with_windows s;

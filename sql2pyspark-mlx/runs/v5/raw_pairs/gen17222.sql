with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        round(quantity, 2) as quantity_rounded
    from inventory
    where quantity is not null
),
joined as (
    -- Attach stores attributes
    select
        src.id,
        src.store_id,
        src.quantity,
        src.last_restock,
        src.quantity_rounded,
        b.store_name,
        b.sqft
    from base src
    left join stores b
        on src.store_id = b.id
),
joined_inventory as (
    -- Attach stores attributes
    select
        s.id,
        s.store_id,
        s.quantity_rounded,
        s.sqft,
        p2.store_name as store_store_name,
        p2.city,
        p2.opened_date
    from joined s
    left join stores p2
        on s.store_id = p2.id
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        min(sqft) over w_part as running_min_sqft,
        max(sqft) over w_part as running_max_sqft,
        sum(sqft) over w_part as running_sum_sqft,
        avg(sqft) over w_part as running_avg_sqft
    from joined_inventory
    window w_part as (partition by city, store_id order by opened_date asc, id asc rows between unbounded preceding and current row)
),
with_windows_inventory as (
    -- Window metrics over several different partitions
    select
        *,
        lag(running_min_sqft) over (partition by store_id order by running_max_sqft asc, id asc) as prev_running_min_sqft,
        lag(running_avg_sqft) over (partition by store_id order by running_max_sqft asc, id asc) as prev_running_avg_sqft,
        lead(running_avg_sqft) over (partition by store_id order by running_max_sqft asc, id asc) as next_running_avg_sqft,
        row_number() over (partition by store_id order by running_max_sqft asc, id asc) as seq_no
    from with_windows
)
select
    s.id,
    s.store_id,
    s.quantity_rounded,
    s.sqft,
    s.store_store_name,
    s.city,
    s.running_max_sqft,
    s.running_avg_sqft,
    s.prev_running_min_sqft,
    (select sum(x.discount) from orders x where x.store_id = s.store_id) as sum_order_discount
from with_windows_inventory s
order by id asc;

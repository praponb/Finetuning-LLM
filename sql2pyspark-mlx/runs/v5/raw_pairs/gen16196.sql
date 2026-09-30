with cleaned as (
    select
        id,
        store_id,
        product_id,
        quantity,
        coalesce(quantity, 0) as quantity_filled,
        date_format(last_restock, 'yyyy-MM') as last_restock_label
    from inventory
    where last_restock >= '2024-03-01'
),
metrics as (
    select
        id,
        store_id,
        quantity,
        quantity_filled,
        last_restock_label,
        lag(quantity_filled) over w_hist as prev_quantity_filled,
        lead(quantity_filled) over w_hist as next_quantity_filled,
        lag(quantity) over w_hist as prev_quantity,
        lead(quantity) over w_hist as next_quantity,
        row_number() over w_hist as event_seq
    from cleaned
    where quantity_filled < 38
    window w_hist as (partition by product_id, store_id order by quantity_filled, id)
),
remaining_keys as (
    select id, store_id
    from metrics
    intersect all
    select id, store_id
    from inventory
    where quantity < 70
),
verified as (
    select s.*
    from metrics s
    join remaining_keys k on s.id = k.id and s.store_id = k.store_id
),
with_windows as (
    select
        store_id,
        quantity,
        last_restock_label,
        next_quantity_filled,
        next_quantity,
        event_seq,
        cume_dist() over w_hist as next_quantity_filled_cume_dist,
        percent_rank() over (partition by store_id order by prev_quantity) as prev_quantity_percent_rank,
        max(next_quantity) over (partition by store_id) as store_id_max_next_quantity
    from verified
    window w_hist as (partition by store_id order by next_quantity_filled desc)
),
enriched as (
    select
        *,
        cume_dist() over (partition by store_id order by next_quantity_filled_cume_dist desc) as next_quantity_filled_cume_dist_cume_dist,
        max(event_seq) over (partition by store_id) as store_id_max_event_seq,
        min(next_quantity_filled_cume_dist) over (partition by store_id) as store_id_min_next_quantity_filled_cume_dist,
        count(next_quantity_filled_cume_dist) over (partition by store_id) as store_id_count_next_quantity_filled_cume_dist
    from with_windows
),
kept_keys as (
    select store_id
    from enriched
    except distinct
    select store_id
    from inventory
    where store_id = 5
),
reconciled as (
    select s.*
    from enriched s
    join kept_keys k on s.store_id = k.store_id
),
report as (
    select
        store_id,
        next_quantity_filled,
        next_quantity,
        event_seq,
        next_quantity_filled_cume_dist,
        store_id_max_next_quantity,
        next_quantity_filled_cume_dist_cume_dist,
        store_id_min_next_quantity_filled_cume_dist,
        store_id_count_next_quantity_filled_cume_dist
    from reconciled
)
select
    s.store_id,
    s.next_quantity_filled,
    s.next_quantity,
    s.event_seq,
    s.next_quantity_filled_cume_dist,
    s.store_id_max_next_quantity,
    s.next_quantity_filled_cume_dist_cume_dist,
    s.store_id_min_next_quantity_filled_cume_dist,
    s.store_id_count_next_quantity_filled_cume_dist,
    (select max(x.quantity) from inventory x where x.store_id = s.store_id) as max_inventory_quantity
from report s
order by max_inventory_quantity nulls first

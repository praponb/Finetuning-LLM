with deduped as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by store_id, product_id order by last_restock desc nulls first, id) as rn
        from inventory
    ) t
    where rn = 1
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        lag(quantity, 1, 0) over w_grp as prev_quantity,
        ntile(3) over w_recent as quantity_bucket,
        lead(quantity, 1) over w_hist as next_quantity,
        percent_rank() over (partition by store_id order by quantity nulls first) as quantity_percent_rank
    from deduped
    window w_grp as (partition by product_id order by quantity, id), w_recent as (order by quantity desc, id), w_hist as (order by rn, id)
),
deduped_inventory as (
    -- QUALIFY-style window filter
    select *
    from metrics
    qualify rank() over (partition by store_id order by rn) = 1
),
deduped_inventory_2 as (
    -- Keep one row per key (latest first)
    select *
    from deduped_inventory
    qualify row_number() over (partition by store_id order by next_quantity, id) = 1
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        round(quantity_bucket / sum(quantity_bucket) over (partition by product_id, store_id), 4) as quantity_bucket_share,
        avg(quantity_bucket) over (partition by product_id order by next_quantity, id rows between 5 preceding and current row) as quantity_bucket_avg_5,
        sum(quantity_bucket) over (partition by product_id order by last_restock, id rows between unbounded preceding and current row) as cumulative_quantity_bucket,
        percent_rank() over (partition by product_id, store_id order by quantity nulls first) as quantity_percent_rank_2
    from deduped_inventory_2
),
enriched as (
    -- Ranking and gap features
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        rn,
        prev_quantity,
        quantity_bucket,
        next_quantity,
        quantity_percent_rank,
        quantity_bucket_share,
        quantity_bucket_avg_5,
        cumulative_quantity_bucket,
        quantity_percent_rank_2,
        first_value(quantity_bucket, true) over (partition by store_id order by next_quantity, id rows between unbounded preceding and current row) as first_known_quantity_bucket,
        ntile(3) over (partition by product_id order by quantity_percent_rank_2 desc, id) as quantity_percent_rank_2_bucket,
        percent_rank() over (partition by store_id, product_id order by quantity_percent_rank desc nulls last) as quantity_percent_rank_percent_rank,
        lag(quantity_bucket_share, 2) over w_running as prev_quantity_bucket_share
    from scored
    window w_running as (partition by store_id order by rn, id)
),
report as (
    select
        id,
        product_id,
        quantity,
        rn,
        prev_quantity,
        quantity_percent_rank,
        quantity_percent_rank_2,
        first_known_quantity_bucket,
        quantity_percent_rank_2_bucket
    from enriched
)
select
    s.id,
    s.product_id,
    s.quantity,
    s.rn,
    s.prev_quantity,
    s.quantity_percent_rank,
    s.quantity_percent_rank_2,
    s.first_known_quantity_bucket,
    s.quantity_percent_rank_2_bucket,
    (select min(t2.units) from store_sales t2 where t2.product_id = s.product_id) as min_store_sale_units
from report s;

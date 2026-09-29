with scored as (
    -- Window metrics over several different partitions
    select
        *,
        round(quantity / sum(quantity) over (partition by product_id), 4) quantity_share,
        row_number() over (order by unit_price desc, id) seq_no,
        ntile(4) over (partition by product_id order by quantity desc, id) quantity_bucket,
        cume_dist() over (partition by order_id, product_id order by quantity desc nulls first) quantity_cume_dist
    from order_items
    where order_id in (2, 9, 31)
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        product_id,
        unit_price,
        quantity_share,
        seq_no,
        quantity_bucket,
        quantity * quantity_bucket quantity_times_quantity_bucket
    from scored
    where product_id is not null
),
latest as (
    -- QUALIFY-style window filter
    select
        id,
        unit_price,
        quantity_share,
        seq_no,
        quantity_bucket,
        quantity_times_quantity_bucket
    from (
        select *, row_number() over (partition by order_id, product_id order by quantity_times_quantity_bucket nulls last, id) as dedup_rank
        from valid
    ) ranked
    where dedup_rank = 1
),
scored_order_items as (
    -- Window metrics over several different partitions
    select
        id,
        seq_no,
        quantity_times_quantity_bucket,
        ntile(10) over (order by seq_no, id) seq_no_bucket,
        lead(unit_price, 1, 0) over (order by quantity_share, id) next_unit_price,
        last_value(quantity_share) ignore nulls over (order by quantity_share, id rows between unbounded preceding and current row) last_known_quantity_share
    from latest
)
-- Aggregate with conditional metrics
select
    seq_no,
    count(*) as event_count,
    percentile_approx(seq_no, array(0.25, 0.5, 0.75)) as seq_no_quartiles,
    round(avg(last_known_quantity_share), 2) as avg_last_known_quantity_share,
    approx_percentile(quantity_times_quantity_bucket, 0.9) as quantity_times_quantity_bucket_p90,
    percentile_approx(seq_no, 0.5) as seq_no_p50
from scored_order_items
group by 1
order by seq_no_p50 desc nulls last;

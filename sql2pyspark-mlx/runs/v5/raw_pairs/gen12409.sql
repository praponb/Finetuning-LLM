with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        quantity,
        unit_price * quantity as line_total
    from order_items
    where product_id is not null
      and order_id in (10, 37)
),
ranked as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by order_id order by line_total nulls last) as row_num
    from filtered
    qualify row_num = 1
),
joined as (
    -- Attach orders attributes
    select a.*, p.shipping_fee
    from ranked a
    left join orders p
        on a.order_id = p.id
    where p.amount between 900 and 1330
),
with_dims as (
    -- Attach orders attributes
    select src.*, o.status
    from joined src
    left join orders o
        on src.order_id = o.id
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        quantity,
        line_total,
        row_num,
        shipping_fee,
        coalesce(line_total, 0) as line_total_filled,
        substring(status, 1, 4) as status_prefix
    from with_dims
    where row_num <= 1
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        ntile(10) over w_running as line_total_filled_bucket,
        row_number() over (partition by order_id order by line_total, id) as row_seq,
        rank() over w_part as line_total_filled_rank,
        dense_rank() over (partition by order_id order by shipping_fee desc nulls first) as shipping_fee_dense_rank,
        ntile(10) over w_running as line_total_filled_bucket_2
    from cleaned
    window w_running as (order by line_total_filled, id), w_part as (partition by order_id order by line_total_filled desc nulls last)
),
daily as (
    -- Multi-level aggregation
    select
        order_id,
        count(*) as num_rows,
        count(distinct id) as distinct_ids,
        approx_percentile(line_total_filled_bucket_2, array(0.25, 0.5, 0.75)) as line_total_filled_bucket_2_quartiles,
        approx_percentile(line_total_filled_bucket, 0.9) as line_total_filled_bucket_p90
    from with_windows
    group by order_id
)
select
    s.order_id,
    s.num_rows,
    s.line_total_filled_bucket_2_quartiles,
    s.line_total_filled_bucket_p90,
    (select avg(s2.cost) from shipments s2 where s2.order_id = s.order_id) as avg_shipment_cost
from daily s;

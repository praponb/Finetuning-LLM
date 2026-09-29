with filtered as (
    -- Basic cleansing and defaults
    select *, cast(unit_price as int) as unit_price_int
    from order_items
    where product_id is not null
      and product_id = 35
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by order_id order by unit_price_int desc, id) as row_num
    from filtered
    qualify row_num = 1
),
scored as (
    -- Running and moving aggregates
    select
        *,
        lead(quantity) over (partition by product_id order by unit_price_int, id) as next_quantity,
        lag(unit_price_int) over (partition by product_id order by unit_price_int, id) as prev_unit_price_int,
        row_number() over (partition by product_id order by unit_price_int, id) as event_seq,
        lag(unit_price, 2, 0) over (partition by product_id order by row_num, id) as prev_unit_price
    from latest
),
scored_order_items as (
    -- Running and moving aggregates
    select
        id,
        quantity,
        unit_price,
        row_num,
        next_quantity,
        prev_unit_price_int,
        event_seq,
        prev_unit_price,
        avg(unit_price) over (partition by order_id order by prev_unit_price_int, id rows between 5 preceding and current row) as unit_price_avg_5,
        cume_dist() over (order by prev_unit_price_int desc nulls last) as prev_unit_price_int_cume_dist,
        rank() over (partition by product_id, order_id order by event_seq nulls first) as event_seq_rank
    from scored
    where product_id is null
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        quantity,
        unit_price,
        row_num,
        next_quantity,
        prev_unit_price_int,
        event_seq,
        prev_unit_price,
        event_seq_rank,
        unit_price_avg_5 + event_seq as unit_price_avg_5_plus_event_seq,
        coalesce(quantity, 0) as quantity_filled
    from scored_order_items
    where prev_unit_price_int_cume_dist <= 0.31
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        next_quantity,
        prev_unit_price_int,
        event_seq,
        event_seq_rank,
        unit_price_avg_5_plus_event_seq,
        last_value(prev_unit_price_int) ignore nulls over (order by unit_price_avg_5_plus_event_seq, id rows between unbounded preceding and current row) as last_known_prev_unit_price_int,
        ntile(5) over (order by next_quantity, id) as next_quantity_bucket,
        sum(quantity) over w_hist as cumulative_quantity
    from valid
    window w_hist as (order by next_quantity, id rows between unbounded preceding and current row)
),
grouped as (
    -- Grouped metrics
    select
        case when last_known_prev_unit_price_int >= 88 then 'PREMIUM' else 'STANDARD' end as last_known_prev_unit_price_int_tier,
        count(*) as cnt,
        approx_percentile(cumulative_quantity, 0.5) as cumulative_quantity_p50,
        round(avg(last_known_prev_unit_price_int), 2) as avg_last_known_prev_unit_price_int,
        percentile_approx(unit_price_avg_5_plus_event_seq, 0.5) as unit_price_avg_5_plus_event_seq_p50,
        round(avg(next_quantity), 2) as avg_next_quantity,
        count(distinct id) as distinct_ids
    from metrics
    group by 1
)
select
    last_known_prev_unit_price_int_tier,
    cnt,
    cumulative_quantity_p50,
    unit_price_avg_5_plus_event_seq_p50,
    avg_next_quantity
from grouped
order by last_known_prev_unit_price_int_tier;

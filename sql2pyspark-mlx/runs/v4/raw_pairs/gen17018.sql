with scored as (
    -- Ranking and gap features
    select
        *,
        sum(unit_price) over (partition by order_id order by quantity, id rows between unbounded preceding and current row) as running_unit_price,
        cume_dist() over (partition by order_id order by unit_price nulls last) as unit_price_cume_dist,
        avg(unit_price) over (partition by order_id, product_id order by unit_price, id rows between 3 preceding and current row) as unit_price_avg_3,
        row_number() over (partition by order_id order by unit_price desc, id) as seq_no
    from order_items
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by order_id order by unit_price, id) as row_seq,
        sum(running_unit_price) over w_hist as product_id_sum_running_unit_price,
        lag(seq_no, 2) over (partition by order_id order by unit_price_cume_dist, id) as prev_seq_no
    from scored
    window w_hist as (partition by product_id, order_id)
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from with_windows
    qualify row_number() over (partition by order_id, product_id order by prev_seq_no desc, id) = 1
),
enriched as (
    -- Attach products attributes
    select e.*, b.cost
    from ranked e
    left join products b
        on e.product_id = b.id
),
scored_order_items as (
    -- Window metrics over several different partitions
    select
        *,
        lead(row_seq, 1) over (order by unit_price_avg_3, id) as next_row_seq,
        round(cost / sum(cost) over (partition by order_id), 4) as cost_share
    from enriched
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over (partition by product_id order by quantity desc) as quantity_percent_rank,
        avg(unit_price_avg_3) over (partition by order_id) as order_id_avg_unit_price_avg_3,
        lag(seq_no, 2) over w_hist as prev_seq_no_2
    from scored_order_items
    window w_hist as (partition by order_id order by unit_price, id)
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        order_id,
        unit_price_cume_dist,
        unit_price,
        quantity_percent_rank
    from metrics
) src
unpivot include nulls (
    value for metric_name in (unit_price_cume_dist, unit_price, quantity_percent_rank)
);

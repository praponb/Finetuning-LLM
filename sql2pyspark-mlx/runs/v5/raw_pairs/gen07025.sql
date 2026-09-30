with enriched as (
    -- Running and moving aggregates
    select
        id,
        product_id,
        quantity,
        min(unit_price) over (partition by product_id order by quantity, id rows between unbounded preceding and current row) running_min_unit_price,
        count(unit_price) over (partition by product_id order by quantity, id rows between unbounded preceding and current row) running_count_unit_price,
        max(unit_price) over (partition by product_id order by quantity, id rows between unbounded preceding and current row) running_max_unit_price,
        percent_rank() over (partition by product_id order by unit_price desc nulls last) unit_price_percent_rank
    from order_items
),
scored as (
    -- Running and moving aggregates
    select
        *,
        lead(unit_price_percent_rank) over (partition by product_id order by quantity, id) next_unit_price_percent_rank,
        lead(running_count_unit_price) over (partition by product_id order by quantity, id) next_running_count_unit_price,
        lag(running_count_unit_price) over (partition by product_id order by quantity, id) prev_running_count_unit_price,
        row_number() over (partition by product_id order by quantity, id) seq_no
    from enriched
),
enriched_order_items as (
    -- Window metrics over several different partitions
    select
        *,
        lag(unit_price_percent_rank, 2) over w_hist prev_unit_price_percent_rank,
        row_number() over (partition by product_id order by running_min_unit_price, id) event_seq,
        max(prev_running_count_unit_price) over w_grp product_id_max_prev_running_count_unit_price,
        row_number() over (partition by product_id order by unit_price_percent_rank, id) seq_no_2,
        min(seq_no) over w_grp product_id_min_seq_no
    from scored
    window w_hist as (order by running_max_unit_price, id), w_grp as (partition by product_id)
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by product_id order by running_min_unit_price nulls first, id) as rn
        from enriched_order_items
    ) ranked
    where rn = 1
),
latest as (
    -- Keep one row per key (latest first)
    select
        id,
        product_id,
        running_count_unit_price,
        next_unit_price_percent_rank,
        next_running_count_unit_price,
        prev_unit_price_percent_rank,
        event_seq,
        product_id_max_prev_running_count_unit_price,
        rn
    from (
        select *, row_number() over (partition by product_id order by quantity desc, id) as rank_in_group
        from ranked
    ) x
    where rank_in_group = 1
)
-- Grouped metrics
select
    case when next_unit_price_percent_rank >= 0.67 then 'HIGH' else 'LOW' end as next_unit_price_percent_rank_tier,
    case when next_running_count_unit_price >= 15 then 'PREMIUM' else 'STANDARD' end as next_running_count_unit_price_tier,
    count(*) as cnt,
    count(*) as num_rows,
    round(avg(prev_unit_price_percent_rank), 2) as avg_prev_unit_price_percent_rank,
    dense_rank() over (partition by case when next_unit_price_percent_rank >= 0.67 then 'HIGH' else 'LOW' end order by count(*) desc) as cnt_dense_rank
from latest
group by case when next_unit_price_percent_rank >= 0.67 then 'HIGH' else 'LOW' end, case when next_running_count_unit_price >= 15 then 'PREMIUM' else 'STANDARD' end
order by next_unit_price_percent_rank_tier desc nulls last;

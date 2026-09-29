with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        coalesce(unit_price, 0) as unit_price_filled,
        round(quantity, 0) as quantity_rounded
    from order_items
    where product_id is not null
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price_filled,
        quantity_rounded,
        first_value(quantity_rounded) ignore nulls over w_part as first_known_quantity_rounded,
        lag(unit_price_filled, 1) over w_grp as prev_unit_price_filled,
        avg(quantity) over (partition by order_id order by quantity_rounded, id rows between 5 preceding and current row) as quantity_avg_5,
        sum(unit_price_filled) over w_part_2 as running_unit_price_filled,
        ntile(4) over (partition by product_id order by unit_price, id) as unit_price_bucket
    from valid
    window w_part as (partition by product_id order by unit_price, id rows between unbounded preceding and current row), w_grp as (partition by product_id order by quantity, id), w_part_2 as (partition by product_id order by quantity, id rows between unbounded preceding and current row)
),
matched_keys as (
    -- Set difference on keys
    select id, product_id
    from with_windows
    except
    select id, product_id
    from order_items
    where product_id is not null
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    join matched_keys k on s.id = k.id and s.product_id = k.product_id
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price_filled,
        quantity_rounded,
        first_known_quantity_rounded,
        prev_unit_price_filled,
        quantity_avg_5,
        running_unit_price_filled,
        unit_price_bucket,
        round(quantity / sum(quantity) over (partition by product_id), 4) as quantity_share,
        stddev_samp(running_unit_price_filled) over w_recent as order_id_stddev_samp_running_unit_price_filled,
        rank() over w_grp as running_unit_price_filled_rank,
        percent_rank() over (partition by product_id, order_id order by prev_unit_price_filled) as prev_unit_price_filled_percent_rank
    from kept_rows
    window w_recent as (partition by order_id), w_grp as (partition by order_id order by running_unit_price_filled desc nulls last)
),
metrics_order_items as (
    -- Ranking and gap features
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price_filled,
        quantity_rounded,
        first_known_quantity_rounded,
        prev_unit_price_filled,
        quantity_avg_5,
        running_unit_price_filled,
        unit_price_bucket,
        quantity_share,
        order_id_stddev_samp_running_unit_price_filled,
        running_unit_price_filled_rank,
        prev_unit_price_filled_percent_rank,
        rank() over (order by quantity desc nulls last) as quantity_rank,
        round(unit_price_bucket / sum(unit_price_bucket) over (partition by product_id), 4) as unit_price_bucket_share
    from metrics
),
daily as (
    -- Grouped metrics
    select
        case when running_unit_price_filled >= 404 then 'PREMIUM' else 'STANDARD' end as running_unit_price_filled_tier,
        case when running_unit_price_filled_rank >= 7 then 'LARGE' else 'SMALL' end as running_unit_price_filled_rank_tier,
        case when quantity >= 8 then 'HIGH' else 'LOW' end as quantity_tier,
        count(*) as cnt,
        avg(order_id_stddev_samp_running_unit_price_filled) as avg_order_id_stddev_samp_running_unit_price_filled,
        count(distinct order_id) as distinct_order_ids
    from metrics_order_items
    group by 1, 2, 3
)
select
    running_unit_price_filled_tier,
    running_unit_price_filled_rank_tier,
    quantity_tier,
    cnt,
    avg_order_id_stddev_samp_running_unit_price_filled,
    distinct_order_ids
from daily
order by avg_order_id_stddev_samp_running_unit_price_filled nulls first;

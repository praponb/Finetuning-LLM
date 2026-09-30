with ranked as (
    -- Keep one row per key (latest first)
    select *
    from order_items
    qualify row_number() over (partition by order_id order by quantity desc nulls last, id) = 1
),
latest as (
    -- QUALIFY-style window filter
    select
        id,
        product_id,
        unit_price
    from ranked
    qualify row_number() over (partition by order_id, product_id order by quantity desc, id) = 1
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        unit_price,
        sum(unit_price) over w_part as running_unit_price,
        avg(unit_price) over (partition by product_id) as product_id_avg_unit_price,
        first_value(unit_price) ignore nulls over w_part as first_known_unit_price,
        lead(unit_price, 1) over (partition by product_id order by unit_price, id) as next_unit_price,
        round(unit_price / sum(unit_price) over (partition by product_id), 4) as unit_price_share
    from latest
    window w_part as (partition by product_id order by unit_price, id rows between unbounded preceding and current row)
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        unit_price,
        running_unit_price,
        first_known_unit_price,
        next_unit_price,
        unit_price_share,
        first_known_unit_price + product_id_avg_unit_price as first_known_unit_price_plus_product_id_avg_unit_price,
        running_unit_price * product_id_avg_unit_price as running_unit_price_times_product_id_avg_unit_price
    from enriched
    where unit_price_share < 0.42
),
filtered_order_items as (
    -- Filter the raw rows and derive helper columns
    select *, least(unit_price_share, running_unit_price) as least_unit_price_share_running_unit_price
    from filtered
    where first_known_unit_price_plus_product_id_avg_unit_price > 64
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        lag(first_known_unit_price) over (order by least_unit_price_share_running_unit_price, id) as prev_first_known_unit_price,
        lag(unit_price_share) over (order by least_unit_price_share_running_unit_price, id) as prev_unit_price_share,
        row_number() over (order by least_unit_price_share_running_unit_price, id) as row_seq,
        sum(next_unit_price) over (order by next_unit_price, id rows between unbounded preceding and current row) as cumulative_next_unit_price
    from filtered_order_items
    where least_unit_price_share_running_unit_price > 21
),
enriched_order_items as (
    -- Window metrics over several different partitions
    select
        id,
        running_unit_price,
        next_unit_price,
        unit_price_share,
        first_known_unit_price_plus_product_id_avg_unit_price,
        running_unit_price_times_product_id_avg_unit_price,
        least_unit_price_share_running_unit_price,
        prev_first_known_unit_price,
        row_seq,
        cumulative_next_unit_price,
        cume_dist() over (order by least_unit_price_share_running_unit_price nulls last) as least_unit_price_share_running_unit_price_cume_dist,
        ntile(10) over (order by prev_unit_price_share desc, id) as prev_unit_price_share_bucket
    from with_windows
)
select
    id,
    first_known_unit_price_plus_product_id_avg_unit_price,
    running_unit_price_times_product_id_avg_unit_price,
    least_unit_price_share_running_unit_price,
    prev_first_known_unit_price,
    row_seq,
    cumulative_next_unit_price,
    least_unit_price_share_running_unit_price_cume_dist
from enriched_order_items
order by id
limit 20;

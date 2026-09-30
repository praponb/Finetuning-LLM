with base as (
    -- Filter the raw rows and derive helper columns
    select *, least(quantity, unit_price) as least_quantity_unit_price
    from order_items
    where product_id is not null
),
enriched as (
    -- Attach orders attributes
    select
        s.*,
        d.discount,
        d.order_date
    from base s
    left join orders d
        on s.order_id = d.id
),
scored as (
    -- Running and moving aggregates
    select
        id,
        quantity,
        discount,
        order_date,
        first_value(least_quantity_unit_price, true) over (partition by order_id, product_id order by quantity, id rows between unbounded preceding and current row) as first_known_least_quantity_unit_price,
        row_number() over w_running as event_seq,
        cume_dist() over (partition by product_id, order_id order by quantity desc) as quantity_cume_dist,
        rank() over w_part as quantity_rank
    from enriched
    window w_running as (order by discount, id), w_part as (partition by product_id order by quantity)
),
enriched_order_items as (
    -- Window metrics over several different partitions
    select
        id,
        discount,
        first_known_least_quantity_unit_price,
        quantity_cume_dist,
        avg(discount) over (order by order_date, id rows between unbounded preceding and current row) as running_avg_discount,
        count(discount) over (order by order_date, id rows between unbounded preceding and current row) as running_count_discount,
        max(discount) over (order by order_date, id rows between unbounded preceding and current row) as running_max_discount,
        row_number() over (order by order_date, id) as event_seq_2
    from scored
    where discount between 0.12 and 0.28
),
enriched_order_items_2 as (
    -- Running and moving aggregates
    select
        id,
        discount,
        running_count_discount,
        running_max_discount,
        event_seq_2,
        row_number() over (order by first_known_least_quantity_unit_price desc, id) as seq_no,
        avg(event_seq_2) over (order by running_max_discount, id rows between 3 preceding and 1 preceding) as event_seq_2_avg_3_prior
    from enriched_order_items
),
base_order_items as (
    -- Basic cleansing and defaults
    select *, seq_no + discount as seq_no_plus_discount
    from enriched_order_items_2
    where seq_no is not null
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        discount,
        running_max_discount,
        event_seq_2,
        seq_no,
        event_seq_2_avg_3_prior,
        cast(event_seq_2 as double) as event_seq_2_str,
        round(seq_no, 2) as seq_no_rounded
    from base_order_items
    where event_seq_2 between 3 and 6
)
select
    id,
    discount,
    running_max_discount,
    event_seq_2,
    event_seq_2_avg_3_prior,
    event_seq_2_str
from valid
order by event_seq_2_avg_3_prior nulls last

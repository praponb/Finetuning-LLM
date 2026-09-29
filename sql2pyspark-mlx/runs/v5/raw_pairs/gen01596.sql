with valid as (
    -- Basic cleansing and defaults
    select *, date_format(order_date, 'MMM yyyy') as order_date_label
    from orders
    where order_date >= date '2023-03-16'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        first_value(amount, true) over (partition by store_id order by order_date, id rows between unbounded preceding and current row) as first_known_amount,
        last_value(channel) ignore nulls over (partition by status order by discount, id rows between unbounded preceding and current row) as last_known_channel,
        rank() over (partition by store_id order by amount desc) as amount_rank
    from valid
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        discount,
        shipping_fee,
        first_known_amount,
        last_known_channel,
        avg(amount) over (partition by status, channel order by shipping_fee, id rows between 2 preceding and current row) as amount_avg_2,
        row_number() over (order by amount, id) as row_seq
    from with_windows
),
remaining_keys as (
    -- Set intersection on keys
    select id
    from enriched
    intersect
    select id
    from orders
    where shipping_fee < 13
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join remaining_keys k on s.id = k.id
),
valid_orders as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        first_known_amount,
        last_known_channel,
        row_seq,
        round(discount * 0.9, 2) as discount_adjusted,
        first_known_amount - shipping_fee as first_known_amount_minus_shipping_fee
    from verified
    where discount <= 0.12
)
select
    id,
    last_known_channel,
    row_seq,
    discount_adjusted
from valid_orders
order by id desc

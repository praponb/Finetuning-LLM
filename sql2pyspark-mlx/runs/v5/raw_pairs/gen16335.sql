with valid as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        status,
        channel,
        amount,
        cast(shipping_fee as int) as shipping_fee_int
    from orders
    where store_id is not null
),
remaining_keys as (
    -- Set intersection on keys
    select status, customer_id
    from valid
    intersect
    select status, customer_id
    from orders
    where status = 'RETURNED'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join remaining_keys k on s.status = k.status and s.customer_id = k.customer_id
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        ceil(amount) as amount_whole,
        coalesce(channel, 'NONE') as channel_filled
    from kept_rows
    where customer_id is not null
),
metrics as (
    -- Window metrics over several different partitions
    select
        customer_id,
        status,
        channel,
        amount,
        shipping_fee_int,
        amount_whole,
        channel_filled,
        cume_dist() over (partition by status, customer_id order by amount desc nulls last) as amount_cume_dist,
        max(amount) over (partition by customer_id) as customer_id_max_amount,
        rank() over (partition by customer_id order by shipping_fee_int asc) as shipping_fee_int_rank,
        dense_rank() over (order by amount desc nulls first) as amount_dense_rank
    from cleaned
    where status = 'PENDING'
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        status,
        channel,
        amount,
        shipping_fee_int,
        amount_whole,
        channel_filled,
        amount_dense_rank,
        round(shipping_fee_int, 0) as shipping_fee_int_rounded,
        trim(channel) as clean_channel,
        coalesce(channel, 'UNKNOWN') as resolved_channel
    from metrics
    where customer_id is not null
      and channel is not distinct from 'store'
)
-- Native SQL PIVOT
select *
from (
    select
        status,
        channel,
        amount_whole
    from filtered
) src
pivot (
    sum(amount_whole) as amount,
    count(1) as cnt
    for channel in ('mobile' as mobile, 'partner' as partner)
);

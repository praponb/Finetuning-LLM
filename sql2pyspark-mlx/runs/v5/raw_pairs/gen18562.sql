with valid as (
    -- Keep recent, valid rows
    select
        *,
        concat(channel, ', ', status) as channel_status,
        trim(channel) as clean_channel,
        coalesce(channel, 'UNKNOWN') as channel_clean
    from orders
    where order_date >= date '2023-07-07'
),
metrics as (
    -- Ranking and gap features
    select
        id,
        discount,
        shipping_fee,
        channel_status,
        max(amount) over (partition by customer_id, channel order by order_date asc, id asc rows between unbounded preceding and current row) as running_max_amount,
        sum(amount) over (partition by customer_id, channel order by order_date asc, id asc rows between unbounded preceding and current row) as running_sum_amount,
        avg(amount) over (partition by customer_id, channel order by order_date asc, id asc rows between unbounded preceding and current row) as running_avg_amount,
        ntile(3) over (partition by customer_id order by shipping_fee asc, id asc) as shipping_fee_bucket
    from valid
),
kept_keys as (
    -- Set intersection on keys
    select id
    from metrics
    intersect all
    select id
    from orders
    where status in ('COMPLETED', 'SHIPPED')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    join kept_keys k on s.id = k.id
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from verified s
    where not exists (
            select 1
            from shipments c
            where c.order_id = s.id
              and c.carrier = 'Flash'
        )
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (order by running_sum_amount asc nulls first) as running_sum_amount_dense_rank,
        percent_rank() over (order by running_max_amount desc nulls first) as running_max_amount_percent_rank
    from kept
    where shipping_fee_bucket is not null
),
kept_orders as (
    -- Semi-join via a correlated subquery
    select s.*
    from scored s
    where exists (
            select 1
            from shipments c
            where c.order_id = s.id
              and c.status in ('LOST', 'RETURNED')
        )
)
select
    id,
    shipping_fee,
    channel_status,
    running_max_amount,
    running_sum_amount_dense_rank,
    running_max_amount_percent_rank
from kept_orders

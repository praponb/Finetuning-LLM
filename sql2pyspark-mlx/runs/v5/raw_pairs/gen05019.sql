with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        order_date,
        status,
        channel,
        discount,
        upper(channel) as channel_upper,
        coalesce(channel, 'UNKNOWN') as resolved_channel
    from orders
    where order_date > to_date('2023-05-31')
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        order_date,
        discount,
        lead(discount) over w_running as next_discount,
        lag(discount) over w_running as prev_discount,
        row_number() over w_running as event_seq
    from cleaned
    window w_running as (partition by status order by order_date, id)
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by customer_id order by next_discount, id) as rank_in_group
        from with_windows
    ) t
    where rank_in_group = 1
),
remaining_keys as (
    -- Set intersection on keys
    select id, customer_id
    from deduped
    intersect
    select id, customer_id
    from orders
    where customer_id = 20
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from deduped s
    join remaining_keys k on s.id = k.id and s.customer_id = k.customer_id
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from verified s
    where not exists (
            select 1
            from payments ch
            where ch.order_id = s.id
              and ch.paid_at <= s.order_date
        )
)
select
    s.id,
    s.customer_id,
    s.order_date,
    s.next_discount,
    s.prev_discount,
    s.event_seq,
    s.rank_in_group,
    (select avg(x.amount) from orders x where x.customer_id = s.customer_id) as avg_order_amount
from kept s

with with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(paid_at) over (partition by status order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by status order by paid_at, id) + interval '7' day then 1 else 0 end as is_new_burst
    from payments
),
sessions_payments as (
    -- Running sum of the flag = session number
    select
        id,
        order_id,
        paid_at,
        status,
        sum(is_new_burst) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_id
    from with_gaps
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by order_id order by session_id desc) as latest_rank
        from sessions_payments
    ) ranked
    where latest_rank = 1
),
joined as (
    -- Attach orders attributes
    select
        s.*,
        d.amount,
        d.channel
    from deduped s
    left join orders d
        on s.order_id = d.id
),
scored as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        status,
        session_id,
        latest_rank,
        channel,
        lead(session_id) over (partition by channel order by latest_rank, id) as next_session_id,
        lag(session_id) over (partition by channel order by latest_rank, id) as prev_session_id,
        lag(amount) over (partition by channel order by latest_rank, id) as prev_amount,
        row_number() over (partition by channel order by latest_rank, id) as event_seq
    from joined
    where latest_rank >= 1
),
joined_payments as (
    -- Attach orders attributes
    select
        src.*,
        o.discount,
        o.amount
    from scored src
    inner join orders o
        on src.order_id = o.id
)
select
    id,
    status,
    session_id,
    latest_rank,
    channel,
    next_session_id,
    prev_session_id,
    prev_amount,
    event_seq,
    case when status rlike '^(FAILED|SUCCESS)$' then 'CORE' when status rlike '^(REFUNDED)$' then 'GROUP_B' else 'OTHER_ZONE' end as status_zone
from joined_payments
order by next_session_id nulls last

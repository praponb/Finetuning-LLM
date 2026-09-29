with base as (
    -- Keep recent, valid rows
    select *, case when method = 'card' then 1 else 0 end as is_card
    from payments
    where method is not null
      and amount < 1590
),
with_prev as (
    -- Previous event time per key
    select *, lag(paid_at) over (partition by order_id order by paid_at asc, id asc) as prev_paid_at
    from base
),
flagged as (
    -- Flag rows that start a new session
    select *, case when prev_paid_at is null or paid_at > prev_paid_at + interval '60' day then 1 else 0 end as new_group_flag
    from with_prev
),
sessions_payments as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by order_id order by paid_at asc, id asc rows between unbounded preceding and current row) as island_id
    from flagged
),
scored as (
    -- Running and moving aggregates
    select
        *,
        lead(is_card) over (partition by status order by amount asc, id asc) as next_is_card,
        lag(is_card) over (partition by status order by amount asc, id asc) as prev_is_card,
        lead(new_group_flag) over (partition by status order by amount asc, id asc) as next_new_group_flag,
        row_number() over (partition by status order by amount asc, id asc) as seq_no,
        ntile(10) over (order by amount desc, id asc) as amount_bucket
    from sessions_payments
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        amount,
        paid_at,
        status,
        prev_paid_at,
        new_group_flag,
        next_is_card,
        prev_is_card,
        seq_no,
        amount_bucket,
        case when status = 'REFUNDED' then 1 else 0 end as is_refunded,
        coalesce(method, 'NONE') as method_filled
    from scored
    where island_id >= 2
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        status,
        new_group_flag,
        amount_bucket,
        method_filled,
        avg(prev_is_card) over w_hist as prev_is_card_avg_3,
        sum(prev_is_card) over w_running as cumulative_prev_is_card,
        sum(amount_bucket) over (partition by status order by is_refunded asc, id asc rows between unbounded preceding and current row) as running_amount_bucket,
        lag(amount_bucket, 1) over (partition by status order by amount_bucket asc, id asc) as prev_amount_bucket
    from filtered
    window w_hist as (order by is_refunded asc, id asc rows between 3 preceding and current row), w_running as (partition by status order by new_group_flag asc, id asc rows between unbounded preceding and current row)
)
select
    id,
    status,
    new_group_flag,
    amount_bucket,
    method_filled,
    prev_is_card_avg_3,
    running_amount_bucket,
    prev_amount_bucket,
    case when status rlike '^(REFUNDED|FAILED)$' then 'CORE' else 'OTHER_ZONE' end as status_zone
from with_windows;

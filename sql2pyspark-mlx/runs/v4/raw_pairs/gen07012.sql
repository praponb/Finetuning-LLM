with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        case when status = 'PAUSED' then 1 else 0 end as is_paused
    from subscriptions
    where end_date > date '2024-04-07'
      and customer_id is not null
      and status in ('EXPIRED', 'ACTIVE')
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        is_paused,
        round(monthly_fee / sum(monthly_fee) over (partition by customer_id), 4) as monthly_fee_share,
        row_number() over (partition by status order by is_paused asc, id asc) as seq_no,
        avg(is_paused) over (partition by customer_id, status order by start_date asc, id asc rows between 6 preceding and current row) as is_paused_avg_6,
        row_number() over (partition by status, customer_id order by is_paused asc, id asc) as event_seq,
        lag(monthly_fee, 1) over (partition by status order by start_date asc, id asc) as prev_monthly_fee
    from cleaned
),
cleaned_subscriptions as (
    -- Basic cleansing and defaults
    select *, seq_no * monthly_fee as seq_no_times_monthly_fee
    from with_windows
    where start_date >= to_date('2024-03-20')
      and customer_id is not null
      and prev_monthly_fee > 160
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from cleaned_subscriptions
    qualify row_number() over (partition by customer_id, status order by seq_no_times_monthly_fee desc nulls last, id asc) = 1
),
by_type as (
    -- Native SQL PIVOT
    select *
    from (
        select
            customer_id,
            status,
            monthly_fee_share
        from deduped
    ) src
    pivot (
        avg(monthly_fee_share) as amount,
        count(1) as cnt
        for status in ('PAUSED' as paused, 'EXPIRED' as expired)
    )
)
select
    customer_id,
    paused_amount,
    paused_cnt,
    expired_amount
from by_type
order by customer_id desc;

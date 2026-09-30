with valid as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        status,
        length(plan) as plan_length
    from subscriptions
    where customer_id is not null
      and start_date < '2024-12-12'
),
scored as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        status,
        row_number() over w_running as event_seq,
        row_number() over (order by plan_length desc, id) as event_seq_2,
        avg(monthly_fee) over w_hist as monthly_fee_avg_6,
        first_value(monthly_fee, true) over (order by monthly_fee, id rows between unbounded preceding and current row) as first_known_monthly_fee,
        round(monthly_fee / sum(monthly_fee) over (partition by plan), 4) as monthly_fee_share
    from valid
    where customer_id = 15
    window w_running as (partition by customer_id, status order by plan_length desc, id), w_hist as (partition by customer_id order by end_date, id rows between 6 preceding and current row)
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        lag(event_seq, 2) over (partition by status, customer_id order by monthly_fee, id) as prev_event_seq,
        sum(monthly_fee) over (partition by plan order by end_date, id rows between unbounded preceding and current row) as running_monthly_fee
    from scored
),
filtered as (
    -- Keep recent, valid rows
    select *, length(plan) as plan_length
    from with_windows
    where end_date > to_date('2023-08-16')
)
-- Native SQL PIVOT
select *
from (
    select
        plan,
        customer_id,
        status,
        first_known_monthly_fee
    from filtered
) src
pivot (
    avg(first_known_monthly_fee)
    for status in ('EXPIRED' as expired, 'CANCELLED' as cancelled)
)

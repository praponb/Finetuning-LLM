with ranked as (
    -- Keep one row per key (latest first)
    select *
    from subscriptions
    qualify row_number() over (partition by customer_id order by monthly_fee desc nulls last, id) = 1
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        end_date,
        monthly_fee,
        status,
        lag(monthly_fee, 2) over (partition by status order by end_date, id) as prev_monthly_fee,
        rank() over (order by monthly_fee desc nulls first) as monthly_fee_rank,
        row_number() over (partition by customer_id order by start_date, id) as seq_no
    from ranked
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        monthly_fee,
        status,
        prev_monthly_fee,
        monthly_fee_rank,
        seq_no,
        prev_monthly_fee + seq_no as prev_monthly_fee_plus_seq_no,
        add_months(end_date, 1) as end_date_plus_months
    from metrics
    where customer_id is not null
),
metrics_subscriptions as (
    -- Ranking and gap features
    select
        id,
        monthly_fee,
        status,
        prev_monthly_fee_plus_seq_no,
        lead(prev_monthly_fee) over (partition by customer_id order by prev_monthly_fee_plus_seq_no, id) as next_prev_monthly_fee,
        lag(prev_monthly_fee) over (partition by customer_id order by prev_monthly_fee_plus_seq_no, id) as prev_prev_monthly_fee,
        lead(monthly_fee) over (partition by customer_id order by prev_monthly_fee_plus_seq_no, id) as next_monthly_fee,
        row_number() over (partition by customer_id order by prev_monthly_fee_plus_seq_no, id) as row_seq,
        first_value(monthly_fee_rank, true) over (partition by status order by prev_monthly_fee, id rows between unbounded preceding and current row) as first_known_monthly_fee_rank
    from cleaned
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        lead(monthly_fee) over (partition by status order by first_known_monthly_fee_rank, id) as next_monthly_fee_2,
        lag(prev_monthly_fee_plus_seq_no) over (partition by status order by first_known_monthly_fee_rank, id) as prev_prev_monthly_fee_plus_seq_no,
        row_number() over (partition by status order by first_known_monthly_fee_rank, id) as row_seq_2
    from metrics_subscriptions
    where first_known_monthly_fee_rank is not null
),
remaining_keys as (
    -- Set difference on keys
    select status
    from with_windows
    except all
    select status
    from subscriptions
    where customer_id = 40
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join remaining_keys k on s.status = k.status
)
select
    monthly_fee,
    status,
    next_prev_monthly_fee,
    prev_prev_monthly_fee,
    next_monthly_fee,
    first_known_monthly_fee_rank,
    case when status rlike '^(CANCELLED|ACTIVE)$' then 'PRIORITY' when status rlike '^(PAUSED|EXPIRED)$' then 'GROUP_A' else 'OTHER_ZONE' end as status_zone
from kept_rows
order by prev_prev_monthly_fee, next_monthly_fee desc;

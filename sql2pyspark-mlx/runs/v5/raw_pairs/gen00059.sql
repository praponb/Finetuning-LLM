with base as (
    -- Basic cleansing and defaults
    select *, case when monthly_fee >= 188 then 'High' when monthly_fee >= 41 then 'Medium' else 'Low' end as monthly_fee_tier
    from subscriptions
    where end_date > date '2023-09-09'
      and customer_id is not null
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_tier,
        upper(status) as status_upper
    from base
    where monthly_fee < 120
      and end_date >= date '2023-10-01'
),
remaining_keys as (
    -- Set difference on keys
    select customer_id, id
    from valid
    except all
    select customer_id, id
    from subscriptions
    where customer_id is null
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join remaining_keys k on s.customer_id = k.customer_id and s.id = k.id
),
metrics as (
    -- Ranking and gap features
    select
        *,
        stddev_samp(monthly_fee) over (partition by customer_id) as customer_id_stddev_samp_monthly_fee,
        dense_rank() over (order by monthly_fee desc nulls first) as monthly_fee_dense_rank,
        round(monthly_fee / sum(monthly_fee) over (partition by customer_id), 4) as monthly_fee_share,
        percent_rank() over w_hist as monthly_fee_percent_rank
    from verified
    where start_date between '2024-02-01' and '2025-06-13'
    window w_hist as (order by monthly_fee nulls last)
),
summary as (
    -- Grouped metrics
    select
        case when monthly_fee >= 186 then 'PREMIUM' else 'STANDARD' end as monthly_fee_tier,
        date_trunc('week', end_date) as week_start,
        count_if(status = 'EXPIRED') as expired_events,
        count(case when status = 'CANCELLED' then 1 end) as cancelled_count
    from metrics
    group by case when monthly_fee >= 186 then 'PREMIUM' else 'STANDARD' end, date_trunc('week', end_date)
)
select
    monthly_fee_tier,
    week_start,
    cancelled_count
from summary
order by monthly_fee_tier

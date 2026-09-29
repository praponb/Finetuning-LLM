with valid as (
    -- Filter the raw rows and derive helper columns
    select *, upper(status) as status_upper
    from subscriptions
    where end_date > date '2023-06-19'
      and customer_id is not null
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        lead(monthly_fee, 1) over (partition by customer_id order by start_date, id) as next_monthly_fee,
        lag(monthly_fee, 1) over (partition by customer_id order by start_date, id) as prev_monthly_fee,
        round(monthly_fee / sum(monthly_fee) over (partition by customer_id), 4) as monthly_fee_share,
        ntile(3) over (order by monthly_fee, id) as monthly_fee_bucket
    from valid
),
scored as (
    -- Running and moving aggregates
    select
        *,
        min(next_monthly_fee) over (partition by status) as status_min_next_monthly_fee,
        row_number() over w_grp as row_seq
    from enriched
    window w_grp as (partition by status, customer_id order by monthly_fee_share desc, id)
),
valid_subscriptions as (
    -- Basic cleansing and defaults
    select *, round(next_monthly_fee / prev_monthly_fee, 2) as next_monthly_fee_per_prev_monthly_fee
    from scored
    where start_date >= date '2023-09-16'
      and customer_id is not null
)
select
    id,
    start_date,
    status,
    next_monthly_fee,
    prev_monthly_fee,
    monthly_fee_share,
    row_seq,
    next_monthly_fee_per_prev_monthly_fee
from valid_subscriptions;

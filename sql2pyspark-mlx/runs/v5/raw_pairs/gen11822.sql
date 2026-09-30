with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        substring(plan, 1, 4) as plan_prefix,
        datediff(start_date, end_date) as days_end_date_to_start_date
    from subscriptions
    where end_date >= date '2023-05-01'
      and customer_id is not null
),
with_dims as (
    -- Attach customers attributes
    select e.*, b.name
    from cleaned e
    inner join customers b
        on e.customer_id <=> b.id
),
metrics as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        plan_prefix,
        days_end_date_to_start_date,
        round(monthly_fee / sum(monthly_fee) over (partition by plan), 4) as monthly_fee_share,
        cume_dist() over w_hist as days_end_date_to_start_date_cume_dist
    from with_dims
    where start_date between '2024-01-19' and '2024-10-11'
    window w_hist as (order by days_end_date_to_start_date desc nulls last)
),
metrics_subscriptions as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        status,
        monthly_fee_share,
        row_number() over (partition by status order by monthly_fee asc, id asc) as row_seq,
        row_number() over (partition by customer_id order by days_end_date_to_start_date desc, id asc) as seq_no
    from metrics
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by status order by seq_no desc nulls last, id asc) as rank_in_group
    from metrics_subscriptions
    qualify rank_in_group = 1
)
-- Native SQL PIVOT
select *
from (
    select
        customer_id,
        status,
        row_seq
    from deduped
) src
pivot (
    max(row_seq) as total
    for status in ('ACTIVE', 'CANCELLED', 'PAUSED')
);

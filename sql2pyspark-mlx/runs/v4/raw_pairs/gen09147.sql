with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        ceil(monthly_fee) monthly_fee_whole
    from subscriptions
    where status = 'CANCELLED'
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        round(monthly_fee / sum(monthly_fee) over (partition by customer_id), 4) monthly_fee_share,
        ntile(3) over (order by monthly_fee, id) monthly_fee_bucket
    from filtered
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from scored
    qualify row_number() over (partition by status order by monthly_fee_share desc nulls last, id) = 1
),
metrics as (
    -- Ranking and gap features
    select
        *,
        ntile(4) over (partition by customer_id order by monthly_fee_whole, id) monthly_fee_whole_bucket,
        round(monthly_fee_bucket / sum(monthly_fee_bucket) over (partition by status), 4) monthly_fee_bucket_share,
        row_number() over (partition by status order by monthly_fee desc, id) event_seq,
        rank() over (partition by customer_id, status order by monthly_fee_share desc nulls first) monthly_fee_share_rank
    from ranked
)
-- Grouped metrics
select
    status,
    customer_id,
    count(*) as event_count,
    count(distinct status) as distinct_status,
    size(collect_set(status)) as num_status,
    count(distinct status) as distinct_status_2,
    count(case when status = 'CANCELLED' then 1 end) as cancelled_count
from metrics
group by cube (status, customer_id)
having size(collect_set(status)) >= 1

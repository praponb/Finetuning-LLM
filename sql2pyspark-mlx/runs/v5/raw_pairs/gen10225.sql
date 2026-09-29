with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        cast(amount as int) as amount_int
    from payments
    where method is not null
      and paid_at > timestamp '2023-08-30 17:00:00'
)
-- Multi-level aggregation
select
    method,
    date_trunc('quarter', paid_at) as quarter_start,
    round(avg(amount_int), 2) as avg_amount_int,
    count(*) filter (where status = 'SUCCESS') as success_rows,
    count_if(status = 'FAILED') as failed_events,
    percent_rank() over (partition by method order by round(avg(amount_int), 2) desc) as avg_amount_int_percent_rank
from cleaned
group by 1, 2
order by method desc;

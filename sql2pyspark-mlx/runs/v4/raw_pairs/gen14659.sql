with filtered as (
    -- Basic cleansing and defaults
    select *, trim(status) as clean_status
    from subscriptions
    where end_date > date '2024-03-07'
      and end_date <= '2025-01-01'
)
-- Grouped metrics
select
    status,
    customer_id,
    stddev_samp(monthly_fee) as monthly_fee_stddev,
    avg(monthly_fee) as avg_monthly_fee,
    sum(case when status = 'ACTIVE' then monthly_fee else 0 end) as active_monthly_fee,
    round(sum(monthly_fee) / nullif(count(case when status = 'ACTIVE' then 1 end), 0), 2) as monthly_fee_per_active,
    sum(case when status = 'EXPIRED' then monthly_fee else 0 end) as expired_monthly_fee,
    size(collect_set(status)) as num_status
from filtered
group by status, customer_id
order by active_monthly_fee desc

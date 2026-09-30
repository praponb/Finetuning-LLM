with valid as (
    select
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status,
        year(start_date) start_date_year
    from subscriptions
    where status in ('ACTIVE', 'EXPIRED', 'PAUSED')
      and end_date >= to_date('2023-05-13')
),
ranked as (
    select *
    from valid
    qualify row_number() over (partition by status order by end_date nulls last, id) = 1
),
metrics as (
    select
        id,
        start_date,
        end_date,
        monthly_fee,
        start_date_year,
        first_value(monthly_fee) ignore nulls over (partition by status order by start_date_year, id rows between unbounded preceding and current row) first_known_monthly_fee,
        ntile(10) over w_running monthly_fee_bucket
    from ranked
    window w_running as (partition by customer_id, status order by monthly_fee, id)
),
cleaned as (
    select
        id,
        start_date,
        monthly_fee,
        start_date_year,
        first_known_monthly_fee,
        monthly_fee_bucket,
        round(monthly_fee, 1) monthly_fee_rounded
    from metrics
    where end_date between '2024-09-07' and '2025-08-01'
),
cleaned_subscriptions as (
    select
        id,
        start_date,
        monthly_fee,
        start_date_year,
        monthly_fee_bucket,
        monthly_fee_rounded,
        monthly_fee - first_known_monthly_fee monthly_fee_minus_first_known_monthly_fee
    from cleaned
    where start_date >= date '2023-07-22'
      and start_date_year between 40 and 58
),
valid_subscriptions as (
    select
        id,
        start_date,
        monthly_fee,
        start_date_year,
        monthly_fee_bucket,
        monthly_fee_rounded,
        monthly_fee_minus_first_known_monthly_fee,
        monthly_fee / nullif(monthly_fee_minus_first_known_monthly_fee, 0) monthly_fee_per_monthly_fee_minus_first_known_monthly_fee
    from cleaned_subscriptions
    where start_date >= to_date('2023-03-15')
      and start_date between '2023-03-12' and '2025-06-01'
)
select
    case when monthly_fee_rounded >= 87 then 'PREMIUM' else 'STANDARD' end as monthly_fee_rounded_tier,
    count(*) as num_rows,
    count(*) as num_rows_2,
    approx_percentile(monthly_fee, 0.9) as monthly_fee_p90
from valid_subscriptions
group by case when monthly_fee_rounded >= 87 then 'PREMIUM' else 'STANDARD' end
order by monthly_fee_p90 nulls first

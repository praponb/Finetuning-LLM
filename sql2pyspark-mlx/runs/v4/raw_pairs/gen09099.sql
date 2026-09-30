with valid as (
    select *, date_trunc('week', start_date) as start_date_period
    from subscriptions
    where customer_id is not null
)
select
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    start_date_period,
    stddev_samp(monthly_fee) over (partition by status, customer_id) as status_stddev_samp_monthly_fee,
    first_value(monthly_fee) ignore nulls over (partition by plan order by end_date, id rows between unbounded preceding and current row) as first_known_monthly_fee,
    percent_rank() over (partition by status order by monthly_fee desc) as monthly_fee_percent_rank
from valid

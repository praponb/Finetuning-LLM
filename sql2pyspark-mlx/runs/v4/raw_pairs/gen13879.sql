with filtered as (
    select
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        coalesce(monthly_fee, 0) as monthly_fee_filled
    from subscriptions
    where start_date > date '2023-11-29'
      and customer_id is not null
),
with_windows as (
    select
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        monthly_fee_filled,
        last_value(monthly_fee) ignore nulls over (partition by customer_id, status order by monthly_fee_filled, id rows between unbounded preceding and current row) as last_known_monthly_fee,
        avg(monthly_fee_filled) over (order by start_date, id rows between 2 preceding and 1 preceding) as monthly_fee_filled_avg_2_prior
    from filtered
),
scored as (
    select
        *,
        ntile(10) over (partition by status order by last_known_monthly_fee, id) as last_known_monthly_fee_bucket,
        first_value(monthly_fee) ignore nulls over (order by monthly_fee_filled, id rows between unbounded preceding and current row) as first_known_monthly_fee,
        row_number() over (order by last_known_monthly_fee, id) as row_seq,
        sum(last_known_monthly_fee) over (partition by status order by monthly_fee, id rows between unbounded preceding and current row) as running_last_known_monthly_fee,
        rank() over (order by monthly_fee_filled desc) as monthly_fee_filled_rank
    from with_windows
)
select
    id,
    start_date,
    monthly_fee_filled,
    last_known_monthly_fee,
    monthly_fee_filled_avg_2_prior,
    first_known_monthly_fee,
    row_seq
from scored
order by id;

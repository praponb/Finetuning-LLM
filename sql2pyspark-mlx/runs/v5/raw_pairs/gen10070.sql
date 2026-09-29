with base as (
    -- Keep recent, valid rows
    select
        *,
        lower(status) as status_lower,
        case when monthly_fee >= 241 then 'High' when monthly_fee >= 44 then 'Medium' else 'Low' end as monthly_fee_band
    from subscriptions
    where end_date > to_date('2023-04-13')
      and customer_id is not null
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        first_value(monthly_fee, true) over (partition by status order by end_date, id rows between unbounded preceding and current row) as first_known_monthly_fee,
        avg(monthly_fee) over (order by end_date, id rows between 2 preceding and 1 preceding) as monthly_fee_avg_2_prior,
        row_number() over (partition by customer_id order by start_date desc, id) as seq_no,
        sum(monthly_fee) over (partition by customer_id order by start_date, id rows between unbounded preceding and current row) as running_monthly_fee,
        round(monthly_fee / sum(monthly_fee) over (partition by status), 4) as monthly_fee_share
    from base
),
enriched_subscriptions as (
    -- Attach customers attributes
    select
        s.*,
        c.signup_date,
        c.age
    from enriched s
    left join customers c
        on s.customer_id = c.id
),
enriched_subscriptions_2 as (
    -- Running and moving aggregates
    select
        id,
        end_date,
        monthly_fee,
        status,
        monthly_fee_band,
        monthly_fee_avg_2_prior,
        signup_date,
        sum(seq_no) over (partition by plan, status) as plan_sum_seq_no,
        lag(monthly_fee_share, 1) over (partition by customer_id order by signup_date, id) as prev_monthly_fee_share,
        row_number() over (partition by status, plan order by age, id) as seq_no_2
    from enriched_subscriptions
)
select
    id,
    end_date,
    monthly_fee,
    status,
    monthly_fee_avg_2_prior,
    signup_date,
    prev_monthly_fee_share
from enriched_subscriptions_2
order by monthly_fee_avg_2_prior desc, end_date desc;

with scored as (
    -- Running and moving aggregates
    select
        *,
        corr(age, credit_limit) over w_part as corr_age_credit_limit,
        ntile(4) over (order by age desc, id) as age_bucket,
        row_number() over w_running as event_seq,
        avg(credit_limit) over (partition by country order by credit_limit, id rows between 3 preceding and 1 preceding) as credit_limit_avg_3_prior,
        min(credit_limit) over (partition by country) as country_min_credit_limit
    from customers
    window w_part as (partition by segment), w_running as (partition by city order by signup_date desc, id)
)
-- Aggregate with conditional metrics
select
    city,
    case when credit_limit >= 14600 then 'HIGH' else 'LOW' end as credit_limit_tier,
    date_trunc('month', signup_date) as activity_month,
    count(*) filter (where country = 'SG') as sg_rows,
    round(avg(event_seq), 2) as avg_event_seq,
    round(sum(credit_limit_avg_3_prior) / nullif(count(case when segment = 'Consumer' then 1 end), 0), 2) as credit_limit_avg_3_prior_per_consumer
from scored
group by 1, 2, 3

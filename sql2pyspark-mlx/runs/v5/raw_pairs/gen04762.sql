with base as (
    -- Basic cleansing and defaults
    select
        *,
        replace(email, ' ', '') email_slug,
        cast(is_active as int) is_active_flag
    from customers
    where city is not null
      and city is null
),
filtered as (
    -- Basic cleansing and defaults
    select *, greatest(age, is_active_flag) greatest_age_is_active_flag
    from base
    where city is distinct from 'London'
      and name like '%Kim%'
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        first_value(is_active_flag, true) over (partition by segment order by greatest_age_is_active_flag, id rows between unbounded preceding and current row) first_known_is_active_flag,
        sum(credit_limit) over (partition by country order by is_active_flag, id rows between unbounded preceding and current row) running_credit_limit,
        avg(credit_limit) over (partition by country order by greatest_age_is_active_flag, id rows between 3 preceding and current row) credit_limit_avg_3
    from filtered
),
latest_match as (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.email,
        s.city,
        s.age,
        s.first_known_is_active_flag,
        s.credit_limit_avg_3,
        latest.subscription_monthly_fee,
        latest.end_date
    from enriched s
    left join lateral (
            select c.monthly_fee as subscription_monthly_fee, c.end_date
            from subscriptions c
            where c.customer_id = s.id
            order by c.monthly_fee desc, c.id desc
            limit 1
        ) latest on true
)
-- Grouped metrics
select
    date_trunc('week', end_date) as week_start,
    approx_percentile(age, array(0.25, 0.5, 0.75)) as age_quartiles,
    round(sum(credit_limit_avg_3) / nullif(count(case when city = 'Bangkok' then 1 end), 0), 2) as credit_limit_avg_3_per_bangkok,
    sum(case when city = 'Bangkok' then age else 0 end) as bangkok_age,
    count(case when city = 'Tokyo' then 1 end) as tokyo_count
from latest_match
group by date_trunc('week', end_date)

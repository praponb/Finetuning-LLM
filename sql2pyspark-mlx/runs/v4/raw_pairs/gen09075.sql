with metrics as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        row_number() over (partition by customer_id, browser order by started_at desc, id) as seq_no,
        first_value(country) ignore nulls over (partition by device order by started_at, id rows between unbounded preceding and current row) as first_known_country
    from sessions
),
attributed as (
    -- Non-equi join on a validity window
    select
        e.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from metrics e
    inner join subscriptions sub
        on e.customer_id = sub.customer_id
       and e.started_at >= sub.start_date
       and e.started_at < sub.end_date + interval '7' day
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from attributed s
    where not exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.page = '/home'
        )
),
enriched as (
    -- Attach customers attributes
    select
        a.*,
        d.signup_date,
        d.credit_limit
    from not_flagged a
    left join customers d
        on a.customer_id = d.id
)
-- Grouped metrics
select
    country,
    date_trunc('month', signup_date) as activity_month,
    round(avg(credit_limit), 2) as avg_credit_limit,
    count(distinct id) as distinct_ids,
    round(avg(subscription_id), 2) as avg_subscription_id,
    count(*) as event_count,
    round(sum(monthly_fee) / nullif(count(case when device = 'mobile' then 1 end), 0), 2) as monthly_fee_per_mobile
from enriched
group by country, date_trunc('month', signup_date)
order by monthly_fee_per_mobile;

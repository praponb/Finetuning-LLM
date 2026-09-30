with cleaned as (
    select *, length(country) as country_length
    from customers
    where signup_date > date '2023-03-23'
      and city is null
),
enriched as (
    select
        id,
        email,
        country,
        is_active,
        lead(credit_limit) over (partition by city, segment order by signup_date asc, id asc) as next_credit_limit,
        lag(credit_limit) over (partition by city, segment order by signup_date asc, id asc) as prev_credit_limit,
        lead(country_length) over (partition by city, segment order by signup_date asc, id asc) as next_country_length,
        row_number() over (partition by city, segment order by signup_date asc, id asc) as row_seq
    from cleaned
    where email like '%@outlook.com'
),
with_latest as (
    select
        s.id,
        s.country,
        s.is_active,
        s.next_credit_limit,
        latest.session_country,
        latest.session_browser
    from enriched s
    left join lateral (
            select c.country as session_country, c.browser as session_browser
            from sessions c
            where c.customer_id = s.id
            order by c.started_at desc, c.id desc
            limit 1
        ) latest on true
),
with_latest_customers as (
    select
        s.id,
        s.country,
        s.session_country,
        s.session_browser,
        top1.start_date,
        top1.subscription_end_date
    from with_latest s
    join lateral (
            select c.start_date, c.end_date as subscription_end_date
            from subscriptions c
            where c.customer_id = s.id
            order by c.monthly_fee desc, c.id desc
            limit 1
        ) top1 on true
),
summary as (
    select
        country,
        size(collect_set(session_browser)) as num_session_browsers,
        count(*) filter (where country = 'DE') as de_rows,
        count(case when session_country = 'SG' then 1 end) as sg_count,
        count(*) filter (where session_browser = 'Firefox') as firefox_rows,
        size(collect_set(session_browser)) as num_session_browsers_2
    from with_latest_customers
    group by country
    having sg_count >= 1
)
select
    country,
    sg_count,
    firefox_rows,
    num_session_browsers_2
from summary

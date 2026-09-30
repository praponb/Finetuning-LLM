with with_latest as (
    select
        s.id,
        s.name,
        s.country,
        s.city,
        s.segment,
        s.signup_date,
        s.is_active,
        s.credit_limit,
        latest.browser,
        latest.session_device
    from customers s
    left join lateral (
            select c.browser, c.device as session_device
            from sessions c
            where c.customer_id = s.id
            order by c.started_at desc, c.id desc
            limit 1
        ) latest on true
)
select
    city,
    date_trunc('year', signup_date) as year_start,
    size(collect_set(city)) as num_cities,
    sum(case when segment = 'Consumer' then credit_limit else 0 end) as consumer_credit_limit,
    avg(credit_limit) filter (where city = 'Bangkok') as bangkok_avg_credit_limit
from with_latest
group by cube (city, date_trunc('year', signup_date))
having consumer_credit_limit >= 0
order by bangkok_avg_credit_limit desc

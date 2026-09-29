with not_flagged as (
    select s.*
    from customers s
    where not exists (
            select 1
            from orders x
            where x.customer_id = s.id
              and x.order_date between '2023-04-01' and '2025-06-11'
        )
),
not_flagged_customers as (
    select s.*
    from not_flagged s
    where not exists (
            select 1
            from accounts c
            where c.customer_id = s.id
              and c.status in ('CLOSED', 'FROZEN')
        )
),
with_windows as (
    select
        *,
        max(age) over (partition by city order by signup_date, id rows between 2 preceding and current row) as last3_max_age,
        min(age) over (partition by city order by signup_date, id rows between 2 preceding and current row) as last3_min_age,
        avg(age) over (partition by city order by signup_date, id rows between 2 preceding and current row) as last3_avg_age,
        lag(age, 1) over (partition by city order by credit_limit, id) as prev_age
    from not_flagged_customers
),
kept as (
    select s.*
    from with_windows s
    where not exists (
            select 1
            from accounts ch
            where ch.customer_id = s.id
              and ch.account_type in ('CHECKING', 'SAVINGS')
        )
),
enriched as (
    select
        *,
        sum(prev_age) over (partition by segment order by last3_avg_age, id rows between unbounded preceding and current row) as cumulative_prev_age,
        ntile(5) over (partition by country order by credit_limit desc, id) as credit_limit_bucket
    from kept
),
latest_match as (
    select s.*, latest.browser
    from enriched s
    left join lateral (
            select c.browser
            from sessions c
            where c.customer_id = s.id
              and c.started_at <= s.signup_date
            order by c.started_at desc, c.id desc
            limit 1
        ) latest on true
)
select
    date_trunc('month', signup_date) as activity_month,
    sort_array(collect_set(browser)) as browser_list,
    sum(case when browser = 'Chrome' then last3_avg_age else 0 end) as chrome_last3_avg_age
from latest_match
group by 1
having chrome_last3_avg_age >= 0

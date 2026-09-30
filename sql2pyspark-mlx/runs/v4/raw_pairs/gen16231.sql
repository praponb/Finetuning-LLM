with with_last as (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.browser,
        top1.session_country
    from customers s
    left join lateral (
            select c.browser, c.country as session_country
            from sessions c
            where c.customer_id = s.id
            order by c.started_at desc, c.id desc
            limit 1
        ) top1 on true
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_last s
    where exists (
            select 1
            from sessions x
            where x.customer_id = s.id
              and x.customer_id in (11, 17, 22, 23)
        )
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from kept s
    where exists (
            select 1
            from sessions ch
            where ch.customer_id = s.id
              and ch.started_at <= s.signup_date
        )
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        round(age / sum(age) over (partition by session_country), 4) as age_share,
        avg(age) over (partition by city order by age, id rows between 6 preceding and current row) as age_avg_6
    from eligible
),
kept_customers as (
    -- Semi-join via a correlated subquery
    select s.*
    from scored s
    where exists (
            select 1
            from sessions c
            where c.customer_id = s.id
              and c.device in ('mobile', 'desktop', 'tablet')
        )
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        ntile(10) over w_part as age_bucket,
        lead(credit_limit, 1) over w_hist as next_credit_limit,
        round(age / sum(age) over (partition by country), 4) as age_share_2,
        ntile(10) over (partition by city, country order by credit_limit desc, id) as credit_limit_bucket,
        max(age_avg_6) over (partition by browser) as browser_max_age_avg_6
    from kept_customers
    window w_part as (partition by city order by age, id), w_hist as (partition by browser order by age_share, id)
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        greatest(age_avg_6, age_bucket) as greatest_age_avg_6_age_bucket,
        concat_ws('/', browser, name) as browser_name_key,
        coalesce(segment, 'OTHER') as segment_filled
    from with_windows
    where signup_date > to_date('2023-11-22')
      and session_country is not null
      and browser = 'Safari'
)
-- Grouped metrics
select
    session_country,
    round(sum(age_bucket) / nullif(count(case when browser = 'Chrome' then 1 end), 0), 2) as age_bucket_per_chrome,
    avg(next_credit_limit) as avg_next_credit_limit,
    count_if(session_country = 'UK') as uk_events,
    sum(case when country = 'SG' then age_share_2 else 0 end) as sg_age_share_2
from cleaned
group by session_country;

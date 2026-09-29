with filtered as (
    select
        id,
        name,
        segment,
        is_active,
        age,
        replace(name, ' ', '_') as name_slug,
        age + credit_limit as age_plus_credit_limit
    from customers
    where signup_date >= date '2023-05-10'
      and email like '%@startup.io'
),
metrics as (
    select
        id,
        segment,
        age,
        name_slug,
        avg(age) over (partition by segment order by age_plus_credit_limit, id rows between 5 preceding and current row) as age_avg_5,
        count(age_plus_credit_limit) over (partition by segment) as segment_count_age_plus_credit_limit,
        row_number() over (order by age_plus_credit_limit, id) as event_seq
    from filtered
    where age >= 51
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by segment order by age desc, id) as row_num
        from metrics
    ) x
    where row_num = 1
),
scored as (
    select
        *,
        lag(age_avg_5) over w_part as prev_age_avg_5,
        lead(age_avg_5) over w_part as next_age_avg_5,
        lag(segment_count_age_plus_credit_limit) over w_part as prev_segment_count_age_plus_credit_limit,
        lead(segment_count_age_plus_credit_limit) over w_part as next_segment_count_age_plus_credit_limit,
        row_number() over w_part as row_seq,
        sum(segment_count_age_plus_credit_limit) over (partition by segment order by event_seq, id rows between unbounded preceding and current row) as cumulative_segment_count_age_plus_credit_limit
    from ranked
    window w_part as (partition by segment order by age, id)
),
not_flagged as (
    select s.*
    from scored s
    where not exists (
            select 1
            from subscriptions ch
            where ch.customer_id = s.id
              and ch.customer_id is null
        )
),
latest_match as (
    select s.*, x.session_browser
    from not_flagged s
    left join lateral (
            select c.browser as session_browser
            from sessions c
            where c.customer_id = s.id
            order by c.started_at desc, c.id desc
            limit 1
        ) x on true
),
with_latest as (
    select
        s.*,
        top1.browser,
        top1.session_device
    from latest_match s
    left join lateral (
            select c.browser, c.device as session_device
            from sessions c
            where c.customer_id = s.id
            order by c.started_at desc, c.id desc
            limit 1
        ) top1 on true
),
totals as (
    select
        session_browser,
        browser,
        bit_or(age) as age_bit_or,
        round(sum(prev_segment_count_age_plus_credit_limit) / nullif(count(case when session_browser = 'Safari' then 1 end), 0), 2) as prev_segment_count_age_plus_credit_limit_per_safari,
        count(case when segment = 'Consumer' then 1 end) as consumer_count
    from with_latest
    group by session_browser, browser
)
select
    session_browser,
    browser,
    age_bit_or,
    prev_segment_count_age_plus_credit_limit_per_safari,
    consumer_count
from totals
order by session_browser, prev_segment_count_age_plus_credit_limit_per_safari desc;

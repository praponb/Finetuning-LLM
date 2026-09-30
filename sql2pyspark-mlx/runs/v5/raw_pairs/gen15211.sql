with cleaned as (
    -- Keep recent, valid rows
    select *, initcap(trim(country)) as country_title
    from sessions
    where started_at >= timestamp '2023-08-14 07:00:00'
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        country_title,
        case when browser in ('Chrome', 'Edge') then 'Group A' else 'Other' end as browser_group,
        case when browser in ('Edge', 'Firefox') then 'Core' else 'Other' end as browser_group_2,
        coalesce(browser, 'OTHER') as browser_filled
    from cleaned
    where country is distinct from 'TH'
),
with_prev as (
    -- Previous event time per key
    select *, lag(started_at) over (partition by country order by started_at asc, id asc) as prev_started_at
    from filtered
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_started_at is null or started_at > prev_started_at + interval '90' day then 1 else 0 end as session_start_flag
    from with_prev
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by country order by started_at asc, id asc rows between unbounded preceding and current row) as session_no
    from with_gaps
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by country order by started_at desc nulls first, id asc) as latest_rank
        from islands
    ) x
    where latest_rank = 1
)
-- Aggregate with conditional metrics
select
    browser,
    device,
    country,
    round(sum(session_no) / nullif(count(case when browser = 'Chrome' then 1 end), 0), 2) as session_no_per_chrome,
    min(started_at) as first_started_at,
    approx_percentile(session_start_flag, 0.5) as session_start_flag_p50
from ranked
group by browser, device, country
order by country asc nulls last

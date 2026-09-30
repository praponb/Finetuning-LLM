with metrics as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        last_value(browser, true) over (partition by customer_id, country order by started_at, id rows between unbounded preceding and current row) as last_known_browser,
        first_value(country) ignore nulls over w_running as first_known_country,
        last_value(country) ignore nulls over w_running as last_known_country,
        row_number() over (partition by device order by started_at desc, id) as row_seq
    from sessions
    window w_running as (partition by country order by started_at, id rows between unbounded preceding and current row)
),
scored as (
    -- Ranking and gap features
    select
        *,
        min(row_seq) over (partition by browser) as browser_min_row_seq,
        lag(row_seq, 2, 0) over (partition by customer_id, country order by started_at, id) as prev_row_seq,
        sum(row_seq) over (partition by browser) as browser_sum_row_seq
    from metrics
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        last_known_browser,
        first_known_country,
        last_known_country,
        row_seq,
        browser_min_row_seq,
        prev_row_seq,
        browser_sum_row_seq,
        datediff(current_date(), started_at) as days_since_started_at,
        coalesce(country, 'DIRECT') as country_filled
    from scored
    where browser is not null
      and to_date(started_at) >= '2025-04-06'
),
by_type as (
    -- Native SQL PIVOT
    select *
    from (
        select
            customer_id,
            device,
            days_since_started_at
        from base
    ) src
    pivot (
        avg(days_since_started_at) as amount
        for device in ('mobile' as mobile, 'desktop' as desktop)
    )
)
select customer_id, desktop
from by_type
order by customer_id desc;

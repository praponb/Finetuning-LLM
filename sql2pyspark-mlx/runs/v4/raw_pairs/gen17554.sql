with base as (
    -- Keep recent, valid rows
    select
        id,
        browser,
        started_at,
        country,
        hour(started_at) as started_at_hour,
        coalesce(country, 'OTHER') as country_filled
    from sessions
    where started_at >= timestamp '2023-07-15 11:00:00'
      and country is not null
      and browser not in ('Safari', 'Firefox')
),
scored as (
    -- Running and moving aggregates
    select
        id,
        browser,
        started_at,
        country,
        started_at_hour,
        country_filled,
        percent_rank() over (partition by country order by started_at_hour desc nulls first) as started_at_hour_percent_rank,
        sum(started_at_hour) over (order by cast(started_at as bigint) range between 604800 preceding and current row) as started_at_hour_sum_7d
    from base
),
scored_sessions as (
    -- Ranking and gap features
    select
        id,
        browser,
        started_at,
        country,
        started_at_hour,
        country_filled,
        started_at_hour_percent_rank,
        started_at_hour_sum_7d,
        sum(started_at_hour) over (partition by browser order by started_at_hour_sum_7d, id rows between unbounded preceding and current row) as cumulative_started_at_hour,
        last_value(started_at_hour_sum_7d, true) over (partition by browser order by started_at_hour, id rows between unbounded preceding and current row) as last_known_started_at_hour_sum_7d,
        count(started_at_hour_sum_7d) over (partition by browser) as browser_count_started_at_hour_sum_7d
    from scored
),
daily as (
    -- Multi-level aggregation
    select
        date_trunc('year', started_at) as year_start,
        browser,
        country,
        sort_array(collect_set(browser)) as browser_list,
        sort_array(collect_set(country)) as country_list,
        percentile_approx(started_at_hour, array(0.25, 0.5, 0.75)) as started_at_hour_quartiles,
        count(*) as row_count
    from scored_sessions
    group by year_start, browser, country
)
select
    year_start,
    browser,
    country,
    browser_list,
    country_list,
    started_at_hour_quartiles,
    row_count
from daily
order by row_count

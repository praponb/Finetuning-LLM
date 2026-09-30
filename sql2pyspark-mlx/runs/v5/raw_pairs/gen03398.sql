with cleaned as (
    -- Basic cleansing and defaults
    select *, coalesce(duration_sec, 0) as duration_sec_filled
    from web_events
    where to_date(event_time) >= '2023-06-01'
      and event_time >= to_date('2023-04-21')
),
joined as (
    -- Attach sessions attributes
    select
        s.*,
        p2.device,
        p2.country
    from cleaned s
    join sessions p2
        on s.session_id = p2.id
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        lead(duration_sec) over w_recent as next_duration_sec,
        lag(duration_sec_filled) over w_recent as prev_duration_sec_filled,
        row_number() over w_recent as seq_no
    from joined
    window w_recent as (partition by event_type order by duration_sec_filled, id)
),
scored_web_events as (
    -- Window metrics over several different partitions
    select
        id,
        event_type,
        page,
        duration_sec_filled,
        country,
        next_duration_sec,
        sum(prev_duration_sec_filled) over w_part as running_sum_prev_duration_sec_filled,
        avg(prev_duration_sec_filled) over w_part as running_avg_prev_duration_sec_filled,
        count(prev_duration_sec_filled) over w_part as running_count_prev_duration_sec_filled,
        ntile(5) over (partition by session_id order by duration_sec_filled desc, id) as duration_sec_filled_bucket
    from scored
    where country = 'TH'
    window w_part as (partition by device order by duration_sec_filled, id rows between unbounded preceding and current row)
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        event_type,
        page,
        country,
        next_duration_sec,
        running_sum_prev_duration_sec_filled,
        running_avg_prev_duration_sec_filled,
        running_count_prev_duration_sec_filled,
        duration_sec_filled_bucket,
        length(country) as country_length
    from scored_web_events
    where running_count_prev_duration_sec_filled between 14 and 15
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, round(country_length, 2) as country_length_rounded
    from filtered
    where country is not null
      and running_count_prev_duration_sec_filled = 16
),
totals as (
    -- Grouped metrics
    select
        country,
        round(sum(country_length_rounded) / nullif(count(case when page = '/search' then 1 end), 0), 2) as country_length_rounded_per_search,
        count(distinct page) as distinct_pages,
        max(country_length_rounded) filter (where country = 'UK') as uk_max_country_length_rounded
    from valid
    group by country
)
select
    country,
    country_length_rounded_per_search,
    distinct_pages,
    uk_max_country_length_rounded
from totals;

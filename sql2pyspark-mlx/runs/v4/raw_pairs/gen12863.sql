with filtered as (
    -- Keep recent, valid rows
    select
        *,
        case when country in ('SG', 'US') then 'Core' else 'Other' end as country_group,
        coalesce(browser, 'NONE') as browser_clean
    from sessions
    where to_date(started_at) < '2024-08-01'
),
metrics as (
    -- Ranking and gap features
    select
        *,
        first_value(browser) ignore nulls over (partition by country order by started_at, id rows between unbounded preceding and current row) as first_known_browser,
        row_number() over (order by started_at, id) as event_seq
    from filtered
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by country order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by country order by started_at, id) + interval 90 days then 1 else 0 end as is_new_burst
    from metrics
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by country order by started_at, id rows between unbounded preceding and current row) as island_id
    from with_gaps
),
matched_keys as (
    -- Set intersection on keys
    select device, country
    from islands
    intersect all
    select device, country
    from sessions
    where country in ('TH', 'US', 'DE')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from islands s
    join matched_keys k on s.device = k.device and s.country = k.country
),
valid as (
    -- Keep recent, valid rows
    select *, case when device = 'tablet' then 1 else 0 end as is_tablet
    from kept_rows
    where started_at >= date '2023-12-08'
      and browser is not null
)
-- Aggregate with conditional metrics
select
    browser,
    round(sum(island_id) / nullif(count(case when browser = 'Safari' then 1 end), 0), 2) as island_id_per_safari,
    sum(case when browser = 'Chrome' then is_new_burst else 0 end) as chrome_is_new_burst,
    approx_percentile(is_new_burst, array(0.25, 0.5, 0.75)) as is_new_burst_quartiles
from valid
group by rollup (browser)
having sum(case when browser = 'Chrome' then is_new_burst else 0 end) > 0
order by browser nulls last;

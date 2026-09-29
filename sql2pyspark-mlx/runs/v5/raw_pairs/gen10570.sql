with valid as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        device,
        browser,
        country,
        case when browser in ('Safari', 'Edge') then 'Core' else 'Other' end as browser_group
    from sessions
    where country is distinct from 'US'
      and browser is not null
),
enriched as (
    -- Attach customers attributes
    select
        src.*,
        d.email,
        d.segment
    from valid src
    join customers d
        on src.customer_id <=> d.id
    where d.country = 'DE'
),
enriched_sessions as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.device,
        a.country,
        a.browser_group,
        a.email,
        p.signup_date,
        p.age,
        p.city
    from enriched a
    join customers p
        on a.customer_id = p.id
),
kept_keys as (
    -- Set intersection on keys
    select device, country
    from enriched_sessions
    intersect
    select device, country
    from sessions
    where hour(started_at) between 6 and 14
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched_sessions s
    join kept_keys k on s.device = k.device and s.country = k.country
),
enriched_sessions_2 as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over w_hist as age_cume_dist,
        min(age) over w_recent as device_min_age,
        rank() over (partition by device order by age desc nulls first) as age_rank,
        cume_dist() over w_part as age_cume_dist_2,
        count(age) over w_hist_2 as city_count_age
    from verified
    window w_hist as (order by age desc), w_recent as (partition by device), w_part as (order by age nulls first), w_hist_2 as (partition by city)
)
-- Join a dimension and aggregate
select
    dim.country as customer_country,
    src.country as session_country,
    count(*) as cnt,
    min(src.device_min_age) as min_device_min_age,
    round(sum(src.city_count_age), 2) as total_city_count_age
from enriched_sessions_2 src
join customers dim on src.customer_id = dim.id
group by grouping sets ((dim.country, src.country), (dim.country), ())
order by min_device_min_age desc nulls last

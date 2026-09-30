with cleaned as (
    select
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        length(name) as name_length
    from customers
    where city not in ('Chiang Mai', 'Tokyo', 'New York')
),
remaining_keys as (
    select id
    from cleaned
    intersect all
    select id
    from customers
    where name like 'Al%'
),
kept_rows as (
    select s.*
    from cleaned s
    inner join remaining_keys k on s.id = k.id
),
metrics as (
    select
        *,
        cume_dist() over w_recent as credit_limit_cume_dist,
        stddev_samp(name_length) over w_hist as city_stddev_samp_name_length
    from kept_rows
    window w_recent as (order by credit_limit desc nulls last), w_hist as (partition by city)
),
valid as (
    select
        *,
        coalesce(name_length, 0) as name_length_filled,
        initcap(trim(name)) as name_title
    from metrics
    where city is not null
),
base as (
    select
        id,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        name_length,
        city_stddev_samp_name_length,
        name_length_filled,
        substring(segment, 1, 3) as segment_prefix,
        name_length_filled + city_stddev_samp_name_length as name_length_filled_plus_city_stddev_samp_name_length,
        coalesce(city, 'N/A') as city_filled
    from valid
    where signup_date >= date '2023-08-04'
),
with_windows as (
    select
        id,
        country,
        name_length,
        city_stddev_samp_name_length,
        name_length_filled_plus_city_stddev_samp_name_length,
        city_filled,
        stddev_samp(city_stddev_samp_name_length) over w_hist as country_stddev_samp_city_stddev_samp_name_length,
        min(credit_limit) over (partition by city) as city_min_credit_limit
    from base
    window w_hist as (partition by country)
)
select
    country,
    sort_array(collect_set(country)) as country_list,
    sort_array(collect_set(country)) as country_list_2,
    count_if(country = 'TH') as th_events,
    grouping_id() as gid
from with_windows
group by cube (country)
having th_events > 1
order by th_events desc

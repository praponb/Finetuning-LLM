with filtered as (
    select
        *,
        length(location) as location_length,
        trim(location) as clean_location,
        coalesce(dept_name, 'NONE') as dept_name_filled
    from departments
    where budget between 2147000 and 3843000
),
metrics as (
    select
        *,
        first_value(dept_name) ignore nulls over w_part as first_known_dept_name,
        avg(location_length) over (partition by location order by budget, id rows between 6 preceding and 1 preceding) as location_length_avg_6_prior
    from filtered
    window w_part as (order by budget, id rows between unbounded preceding and current row)
),
remaining_keys as (
    select dept_name
    from metrics
    except distinct
    select dept_name
    from departments
    where budget < 3618000
),
reconciled as (
    select s.*
    from metrics s
    inner join remaining_keys k on s.dept_name = k.dept_name
),
enriched as (
    select
        id,
        location,
        location_length_avg_6_prior,
        cume_dist() over w_hist as location_length_avg_6_prior_cume_dist,
        sum(location_length) over (partition by location) as location_sum_location_length,
        cume_dist() over (partition by dept_name order by location_length_avg_6_prior nulls first) as location_length_avg_6_prior_cume_dist_2
    from reconciled
    where budget <= 3484000
    window w_hist as (partition by location, dept_name order by location_length_avg_6_prior)
),
eligible as (
    select s.*
    from enriched s
    where not exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.dept_id = 13
        )
),
kept as (
    select s.*
    from eligible s
    where not exists (
            select 1
            from employees x
            where x.dept_id = s.id
              and x.level is not null
        )
)
select
    location,
    count(distinct location) as distinct_locations,
    count_if(location = 'London') as london_events,
    round(sum(location_length_avg_6_prior_cume_dist) / nullif(count(case when location = 'Berlin' then 1 end), 0), 2) as location_length_avg_6_prior_cume_dist_per_berlin,
    sum(case when location = 'New York' then location_length_avg_6_prior_cume_dist else 0 end) as new_york_location_length_avg_6_prior_cume_dist
from kept
group by 1;

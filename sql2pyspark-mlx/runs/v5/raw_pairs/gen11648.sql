with valid as (
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        upper(trim(full_name)) full_name_normalized,
        length(email) email_length,
        coalesce(job_title, 'DIRECT') job_title_filled
    from employees
    where is_remote
),
ranked as (
    select *
    from valid
    qualify row_number() over (partition by level order by hire_date, id) = 1
),
latest as (
    select *, row_number() over (partition by dept_id, level order by email_length desc nulls last, id) as latest_rank
    from ranked
    qualify latest_rank = 1
),
with_windows as (
    select
        id,
        dept_id,
        job_title,
        is_remote,
        full_name_normalized,
        email_length,
        job_title_filled,
        latest_rank,
        stddev_samp(email_length) over (partition by manager_id, level) manager_id_stddev_samp_email_length,
        percent_rank() over (order by email_length desc nulls last) email_length_percent_rank
    from latest
    where full_name is null
)
select
    dept_id,
    avg(email_length) filter (where job_title = 'Director') as director_avg_email_length,
    percentile_approx(email_length, array(0.25, 0.5, 0.75)) as email_length_quartiles,
    count_if(job_title = 'Manager') as manager_events,
    sort_array(collect_set(job_title)) as job_title_list,
    round(avg(email_length), 2) as avg_email_length,
    grouping(dept_id) as grouping_id
from with_windows
group by rollup (dept_id)
order by director_avg_email_length desc nulls last

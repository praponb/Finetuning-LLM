with base as (
    select
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level,
        case when salary >= 154000 then 'High' when salary >= 49000 then 'Medium' else 'Low' end salary_band,
        case when is_remote then 'Yes' else 'No' end is_remote_label,
        coalesce(level, 'OTHER') level_filled
    from employees
    where hire_date > to_date('2023-12-23')
      and level is not null
),
with_windows as (
    select
        *,
        last_value(job_title, true) over (partition by level order by hire_date asc, id asc rows between unbounded preceding and current row) last_known_job_title,
        last_value(level, true) over (partition by level order by hire_date asc, id asc rows between unbounded preceding and current row) last_known_level
    from base
),
with_dims as (
    select
        s.*,
        d.dept_name,
        d.location
    from with_windows s
    left join departments d
        on s.dept_id = d.id
),
stats as (
    select
        date_trunc('month', hire_date) as activity_month,
        count_if(location = 'London') as london_events,
        sort_array(collect_set(location)) as location_list,
        count(case when dept_name = 'HR' then 1 end) as hr_count,
        count(*) as num_rows,
        count(*) as event_count,
        count(distinct dept_name) as distinct_dept_names
    from with_dims
    group by activity_month
)
select
    activity_month,
    london_events,
    location_list,
    hr_count,
    num_rows,
    event_count
from stats
order by event_count asc nulls last;

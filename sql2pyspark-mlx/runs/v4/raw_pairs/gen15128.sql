with ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by job_title order by salary asc, id asc) as rank_in_group
    from employees
    qualify rank_in_group = 1
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        dept_id,
        manager_id,
        salary,
        hire_date,
        rank_in_group,
        cast(is_remote as int) is_remote_flag,
        coalesce(email, 'Unknown') email_filled,
        coalesce(job_title, 'NONE') job_title_clean
    from ranked
    where hire_date > date '2023-09-20'
      and level is not null
      and job_title = 'Analyst'
),
base as (
    -- Keep recent, valid rows
    select *, date_format(hire_date, 'MMM yyyy') hire_date_label
    from filtered
    where hire_date > to_date('2024-01-20')
      and manager_id is not null
),
stats as (
    -- Aggregate with conditional metrics
    select
        date_trunc('month', hire_date) as activity_month,
        count(distinct dept_id) as distinct_dept_ids,
        approx_percentile(salary, array(0.25, 0.5, 0.75)) as salary_quartiles,
        count(distinct manager_id) as distinct_manager_ids,
        percentile_approx(is_remote_flag, array(0.25, 0.5, 0.75)) as is_remote_flag_quartiles,
        count(distinct dept_id) as distinct_dept_ids_2,
        stddev_samp(salary) as salary_stddev
    from base
    group by date_trunc('month', hire_date)
)
select
    activity_month,
    distinct_dept_ids,
    salary_quartiles,
    distinct_manager_ids,
    is_remote_flag_quartiles,
    salary_stddev
from stats;

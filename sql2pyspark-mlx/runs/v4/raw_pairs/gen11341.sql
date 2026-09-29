with filtered as (
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        level,
        cast(is_remote as int) as is_remote_flag,
        coalesce(level, 'OTHER') as resolved_level
    from employees
    where hire_date > date '2024-03-17'
      and dept_id in (20, 38)
),
scored as (
    select
        *,
        rank() over (partition by dept_id order by salary desc) as salary_rank,
        ntile(3) over (partition by dept_id order by salary, id) as salary_bucket,
        avg(is_remote_flag) over (order by is_remote_flag, id rows between 2 preceding and 1 preceding) as is_remote_flag_avg_2_prior,
        ntile(5) over (partition by job_title order by salary, id) as salary_bucket_2,
        lag(is_remote_flag, 2) over (order by is_remote_flag, id) as prev_is_remote_flag
    from filtered
),
base as (
    select
        id,
        email,
        dept_id,
        manager_id,
        salary,
        resolved_level,
        salary_rank,
        salary_bucket,
        is_remote_flag_avg_2_prior,
        salary_bucket_2,
        prev_is_remote_flag,
        substring(job_title, 1, 3) as job_title_prefix,
        coalesce(level, 'N/A') as level_filled
    from scored
    where dept_id = 24
),
joined as (
    select a.*, p.budget
    from base a
    left join departments p
        on a.dept_id = p.id
)
select
    dept_id,
    manager_id,
    count(distinct dept_id) as distinct_dept_ids,
    percentile_approx(salary_bucket, array(0.25, 0.5, 0.75)) as salary_bucket_quartiles
from joined
group by rollup (dept_id, manager_id)
order by distinct_dept_ids desc;

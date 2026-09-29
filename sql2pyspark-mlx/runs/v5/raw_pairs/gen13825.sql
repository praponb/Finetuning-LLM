with valid as (
    select
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary,
        hire_date,
        level,
        case when job_title = 'Engineer' then 1 else 0 end as is_engineer,
        concat_ws('-', email, level) as email_level_key,
        coalesce(level, 'NONE') as level_filled
    from employees
    where job_title = 'Designer'
      and level is distinct from 'L1'
),
joined as (
    select
        s.id,
        s.full_name,
        s.manager_id,
        s.job_title,
        s.salary,
        s.hire_date,
        s.level,
        s.is_engineer,
        s.email_level_key,
        s.level_filled,
        b.salary as employee_salary,
        b.job_title as employee_job_title
    from valid s
    left join employees b
        on s.manager_id = b.id
),
remaining_keys as (
    select job_title, id
    from joined
    except
    select job_title, id
    from employees
    where full_name like 'G%'
),
reconciled as (
    select s.*
    from joined s
    join remaining_keys k on s.job_title = k.job_title and s.id = k.id
),
metrics as (
    select
        id,
        job_title,
        salary,
        hire_date,
        level_filled,
        employee_salary,
        employee_job_title,
        dense_rank() over (partition by manager_id, employee_job_title order by is_engineer desc) as is_engineer_dense_rank,
        cume_dist() over w_grp as salary_cume_dist,
        percent_rank() over (order by salary desc) as salary_percent_rank
    from reconciled
    where manager_id in (10, 16, 29)
    window w_grp as (partition by employee_job_title, job_title order by salary desc)
),
enriched as (
    select
        *,
        percent_rank() over w_grp as salary_percent_rank_2,
        rank() over (partition by job_title order by salary) as salary_rank,
        cume_dist() over (order by salary desc) as salary_cume_dist_2,
        rank() over w_running as employee_salary_rank,
        cume_dist() over w_ordered as salary_cume_dist_cume_dist
    from metrics
    window w_grp as (partition by employee_job_title, job_title order by salary), w_running as (order by employee_salary desc nulls last), w_ordered as (order by salary_cume_dist)
),
base as (
    select
        id,
        job_title,
        salary,
        hire_date,
        level_filled,
        employee_job_title,
        is_engineer_dense_rank,
        salary_cume_dist,
        salary_percent_rank,
        salary_rank,
        salary_cume_dist_2,
        employee_salary_rank,
        salary_cume_dist_cume_dist,
        salary_percent_rank_2 / nullif(salary_percent_rank, 0) as salary_percent_rank_2_per_salary_percent_rank,
        round(salary_percent_rank_2, 1) as salary_percent_rank_2_rounded,
        coalesce(job_title, 'N/A') as job_title_filled
    from enriched
    where employee_job_title is not null
)
select
    date_trunc('week', hire_date) as week_start,
    round(sum(salary_percent_rank_2_per_salary_percent_rank) / nullif(count(case when job_title = 'Manager' then 1 end), 0), 2) as salary_percent_rank_2_per_salary_percent_rank_per_manager,
    avg(salary_cume_dist_cume_dist) as avg_salary_cume_dist_cume_dist,
    percentile_approx(employee_salary_rank, 0.5) as employee_salary_rank_p50,
    count(*) filter (where job_title = 'Manager') as manager_rows,
    size(collect_set(job_title)) as num_job_titles
from base
group by week_start;

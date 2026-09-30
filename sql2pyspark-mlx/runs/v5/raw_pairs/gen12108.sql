with enriched as (
    -- Running and moving aggregates
    select
        *,
        ntile(4) over w_grp as salary_bucket,
        min(salary) over w_grp_2 as dept_id_min_salary,
        lag(salary, 1) over (partition by manager_id order by hire_date, id) as prev_salary
    from employees
    window w_grp as (partition by job_title order by salary, id), w_grp_2 as (partition by dept_id)
)
-- Multi-level aggregation
select
    date_trunc('year', hire_date) as year_start,
    dept_id,
    manager_id,
    count(distinct job_title) as distinct_job_titles,
    size(collect_set(level)) as num_levels
from enriched
group by date_trunc('year', hire_date), dept_id, manager_id
order by num_levels desc nulls last;

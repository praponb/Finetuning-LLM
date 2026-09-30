with base as (
    -- Keep recent, valid rows
    select
        *,
        round(salary * 1.15, 2) as salary_scaled,
        coalesce(level, 'UNKNOWN') as resolved_level
    from employees
    where level is not null
)
-- Multi-level aggregation
select
    date_trunc('month', hire_date) as activity_month,
    size(collect_set(level)) as num_levels,
    sort_array(collect_set(level)) as level_list,
    round(sum(salary) / nullif(count(case when job_title = 'Director' then 1 end), 0), 2) as salary_per_director
from base
group by date_trunc('month', hire_date)
order by salary_per_director desc nulls last

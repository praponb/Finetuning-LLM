with valid as (
    -- Keep recent, valid rows
    select *, coalesce(job_title, 'unassigned') as job_title_filled
    from employees
    where hire_date > to_date('2023-05-08')
      and manager_id is not null
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from valid
    qualify row_number() over (partition by dept_id order by hire_date desc nulls first, id) = 1
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary,
        is_remote,
        avg(salary) over (partition by dept_id order by hire_date, id rows between 2 preceding and current row) as salary_avg_2,
        last_value(job_title) ignore nulls over (order by salary, id rows between unbounded preceding and current row) as last_known_job_title,
        sum(salary) over (partition by level order by hire_date, id rows between unbounded preceding and current row) as cumulative_salary
    from deduped
    where job_title != 'Intern'
),
base as (
    -- Keep recent, valid rows
    select
        *,
        round(salary * 0.9, 2) as salary_adjusted,
        case when is_remote then 'Yes' else 'No' end as is_remote_label
    from metrics
    where job_title is distinct from 'Manager'
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        sum(cumulative_salary) over (partition by job_title, manager_id) as job_title_sum_cumulative_salary,
        sum(cumulative_salary) over (partition by job_title) as job_title_sum_cumulative_salary_2,
        lead(salary_adjusted, 2) over (partition by job_title order by salary, id) as next_salary_adjusted
    from base
),
kept_keys as (
    -- Set difference on keys
    select id, job_title
    from enriched
    except all
    select id, job_title
    from employees
    where job_title not in ('Manager', 'Intern')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join kept_keys k on s.id = k.id and s.job_title = k.job_title
),
summary as (
    -- Grouped metrics
    select
        manager_id,
        approx_percentile(salary_adjusted, 0.5) as salary_adjusted_p50,
        size(collect_set(job_title)) as num_job_titles,
        approx_percentile(job_title_sum_cumulative_salary_2, array(0.25, 0.5, 0.75)) as job_title_sum_cumulative_salary_2_quartiles,
        count(*) filter (where job_title = 'Manager') as manager_rows,
        grouping(manager_id) as grouping_level
    from verified
    group by rollup (manager_id)
)
select
    num_job_titles,
    job_title_sum_cumulative_salary_2_quartiles,
    manager_rows,
    grouping_level
from summary

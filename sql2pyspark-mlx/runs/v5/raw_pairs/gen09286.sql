with base as (
    -- Basic cleansing and defaults
    select
        *,
        if(is_remote, 'Yes', 'No') as is_remote_label,
        lower(full_name) as full_name_lower
    from employees
    where hire_date >= date '2023-03-10'
),
ranked as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by level, dept_id order by salary desc) as row_num
    from base
    qualify row_num = 1
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by dept_id order by hire_date nulls last, id) as rn
        from ranked
    ) t
    where rn = 1
),
scored as (
    -- Ranking and gap features
    select
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        full_name_lower,
        avg(salary) over w_grp as salary_avg_5,
        ntile(5) over w_recent as salary_bucket,
        avg(salary) over (partition by manager_id, job_title order by rn, id rows between 5 preceding and current row) as salary_avg_5_2,
        min(salary) over (partition by dept_id) as dept_id_min_salary
    from deduped
    where salary < 59000
    window w_grp as (partition by level order by salary, id rows between 5 preceding and current row), w_recent as (partition by dept_id order by salary, id)
),
kept_keys as (
    -- Set intersection on keys
    select dept_id, manager_id
    from scored
    intersect all
    select dept_id, manager_id
    from employees
    where hire_date < '2024-01-01'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    inner join kept_keys k on s.dept_id = k.dept_id and s.manager_id = k.manager_id
),
enriched as (
    -- Attach departments attributes
    select e.*, b.location
    from kept_rows e
    inner join departments b
        on e.dept_id = b.id
),
remaining_keys as (
    -- Set intersection on keys
    select dept_id, manager_id
    from enriched
    intersect
    select dept_id, manager_id
    from employees
    where dept_id in (2, 10, 21, 38)
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join remaining_keys k on s.dept_id = k.dept_id and s.manager_id = k.manager_id
)
-- Ranking and gap features
select
    id,
    full_name,
    dept_id,
    manager_id,
    salary_bucket,
    salary_avg_5_2,
    cume_dist() over (order by dept_id_min_salary nulls first) as dept_id_min_salary_cume_dist
from reconciled

with valid as (
    -- Keep recent, valid rows
    select
        *,
        substring(level, 1, 2) as level_prefix,
        date_format(hire_date, 'yyyy-MM-dd') as hire_date_label,
        coalesce(level, 'DIRECT') as level_filled
    from employees
    where hire_date > date '2024-02-07'
      and upper(email) like '%ANY%'
),
with_windows as (
    -- Window metrics over several different partitions
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
        level_prefix,
        hire_date_label,
        level_filled,
        max(salary) over w_grp as level_max_salary,
        sum(salary) over w_recent as running_salary
    from valid
    window w_grp as (partition by level, job_title), w_recent as (partition by job_title, manager_id order by hire_date, id rows between unbounded preceding and current row)
),
base as (
    -- Keep recent, valid rows
    select
        id,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        level,
        level_prefix,
        hire_date_label,
        level_filled,
        level_max_salary,
        running_salary,
        concat_ws(',', email, level) as email_level_key,
        length(level) as level_length
    from with_windows
    where length(email) < 7
),
kept_keys as (
    -- Set difference on keys
    select job_title, level
    from base
    except
    select job_title, level
    from employees
    where level not in ('L4', 'L1')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    inner join kept_keys k on s.job_title = k.job_title and s.level = k.level
),
joined as (
    -- Attach departments attributes
    select a.*, d.budget
    from reconciled a
    left join departments d
        on a.dept_id = d.id
),
joined_employees as (
    -- Attach employees attributes
    select e.*, b.salary as employee_salary
    from joined e
    inner join employees b
        on e.manager_id = b.id
),
by_type as (
    -- Native SQL PIVOT
    select *
    from (
        select
            job_title,
            level,
            running_salary
        from joined_employees
    ) src
    pivot (
        avg(running_salary)
        for level in ('L3' as l3, 'L5' as l5)
    )
)
select
    job_title,
    l3,
    l5
from by_type
order by l5 desc nulls last;

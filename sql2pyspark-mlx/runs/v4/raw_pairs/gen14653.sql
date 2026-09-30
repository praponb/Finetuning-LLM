with base as (
    -- Basic cleansing and defaults
    select
        *,
        substring(dept_name, 1, 2) as dept_name_prefix,
        coalesce(dept_name, 'N/A') as dept_name_clean
    from departments
    where budget <= 3561000
),
with_latest as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.employee_hire_date,
        latest.salary
    from base s
    join lateral (
            select c.hire_date as employee_hire_date, c.salary
            from employees c
            where c.dept_id = s.id
            order by c.hire_date desc, c.id desc
            limit 1
        ) latest on true
),
kept_keys as (
    -- Set difference on keys
    select dept_name, id
    from with_latest
    except all
    select dept_name, id
    from departments
    where dept_name in ('Engineering', 'HR', 'Marketing')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_latest s
    join kept_keys k on s.dept_name = k.dept_name and s.id = k.id
)
-- Native SQL PIVOT
select *
from (
    select
        location,
        dept_name,
        budget
    from verified
) src
pivot (
    sum(budget) as total,
    count(1) as n
    for dept_name in ('Engineering', 'Finance', 'Sales')
)

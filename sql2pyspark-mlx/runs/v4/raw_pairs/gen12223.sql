with valid as (
    select *, upper(location) as location_upper
    from departments
    where dept_name is not null
),
with_last as (
    select s.*, latest.employee_salary
    from valid s
    left join lateral (
            select c.salary as employee_salary
            from employees c
            where c.dept_id = s.id
            order by c.hire_date desc, c.id desc
            limit 1
        ) latest on true
),
with_last_departments as (
    select s.*, x.employee_salary_2
    from with_last s
    join lateral (
            select c.salary as employee_salary_2
            from employees c
            where c.dept_id = s.id
            order by c.hire_date desc, c.id desc
            limit 1
        ) x on true
),
cleaned as (
    select
        id,
        dept_name,
        employee_salary,
        employee_salary_2,
        cast(budget as int) as budget_int,
        coalesce(dept_name, 'UNKNOWN') as dept_name_filled
    from with_last_departments
    where employee_salary between 57000 and 131000
)
select *
from (
    select
        id,
        dept_name,
        employee_salary,
        employee_salary_2
    from cleaned
) src
unpivot (
    val for metric_name in (employee_salary, employee_salary_2)
)

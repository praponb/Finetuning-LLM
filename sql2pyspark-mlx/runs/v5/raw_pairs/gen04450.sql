with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat(location, ' - ', dept_name) as location_dept_name,
        case when dept_name in ('Engineering', 'Sales') then 'Core' else 'Other' end as dept_name_group
    from departments
    where dept_name is not null
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from valid
    qualify row_number() over (partition by dept_name order by budget desc, id) = 1
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        dept_name,
        location_dept_name,
        dept_name_group,
        floor(budget) as budget_whole
    from latest
    where location != 'Berlin'
),
with_last as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.dept_name,
        s.location_dept_name,
        s.budget_whole,
        top1.employee_job_title
    from base s
    left join lateral (
            select c.job_title as employee_job_title
            from employees c
            where c.dept_id = s.id
            order by c.hire_date desc, c.id desc
            limit 1
        ) top1 on true
)
select id, location_dept_name
from with_last
order by id nulls first
limit 20

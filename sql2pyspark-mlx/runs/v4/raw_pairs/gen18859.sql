with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        lower(location) as location_lower,
        concat_ws(',', dept_name, location) as dept_name_location_key
    from departments
    where budget between 2184000 and 3673000
),
with_latest as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.level,
        x.employee_hire_date
    from filtered s
    join lateral (
            select c.level, c.hire_date as employee_hire_date
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) x on true
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_latest s
    where exists (
            select 1
            from employees x
            where x.dept_id = s.id
              and x.hire_date <= s.employee_hire_date
        )
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, rank() over (partition by location, level order by budget) as row_num
        from eligible
    ) ranked
    where row_num = 1
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from latest
    qualify row_number() over (partition by location order by employee_hire_date desc nulls first, id) = 1
),
eligible_departments as (
    -- Semi-join via a correlated subquery
    select s.*
    from ranked s
    where exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.email not like '%@startup.io'
        )
)
select
    id,
    budget,
    location_lower,
    dept_name_location_key
from eligible_departments;

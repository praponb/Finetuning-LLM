with joined as (
    -- Attach employees attributes
    select s.*, p.full_name as employee_full_name
    from employees s
    join employees p
        on s.manager_id = p.id
),
joined_employees as (
    -- Attach departments attributes
    select
        s.id,
        s.full_name,
        s.email,
        s.dept_id,
        s.is_remote,
        s.level,
        s.employee_full_name,
        d.dept_name,
        d.location
    from joined s
    join departments d
        on s.dept_id = d.id
),
with_dims as (
    -- Attach departments attributes
    select
        src.*,
        p.budget,
        p.location as department_location
    from joined_employees src
    join departments p
        on src.dept_id = p.id
),
matched_keys as (
    -- Set difference on keys
    select dept_id, id
    from with_dims
    except
    select dept_id, id
    from employees
    where manager_id is null
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_dims s
    join matched_keys k on s.dept_id = k.dept_id and s.id = k.id
),
enriched as (
    -- Attach departments attributes
    select
        a.id,
        a.full_name,
        a.dept_id,
        a.is_remote,
        a.level,
        a.dept_name,
        a.location,
        a.budget,
        d.dept_name as department_dept_name,
        d.location as department_location_2,
        d.budget as department_budget
    from reconciled a
    left join departments d
        on a.dept_id = d.id
    where d.budget between 2041000 and 3206000
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        full_name,
        level,
        department_dept_name,
        max(budget) over (partition by level, location) as level_max_budget
    from enriched
),
cleaned as (
    -- Basic cleansing and defaults
    select *, case when department_dept_name in ('Engineering', 'HR') then 'Core' else 'Other' end as department_dept_name_group
    from with_windows
    where level is not null
)
select
    id,
    full_name,
    department_dept_name,
    level_max_budget
from cleaned;

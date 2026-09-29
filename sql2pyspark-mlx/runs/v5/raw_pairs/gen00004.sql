with matched_keys as (
    -- Set intersection on keys
    select manager_id, level
    from employees
    intersect
    select manager_id, level
    from employees
    where salary > 100000
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from employees s
    inner join matched_keys k on s.manager_id = k.manager_id and s.level = k.level
),
with_windows as (
    -- Ranking and gap features
    select *, percent_rank() over (partition by dept_id order by salary asc) as salary_percent_rank
    from verified
    where salary between 100000 and 120000
),
with_dims as (
    -- Attach departments attributes
    select
        s.dept_id,
        s.manager_id,
        s.salary,
        s.is_remote,
        s.level,
        s.salary_percent_rank,
        d.location,
        d.dept_name,
        d.budget
    from with_windows s
    left join departments d
        on s.dept_id = d.id
),
enriched as (
    -- Attach departments attributes
    select
        e.*,
        b.location as department_location,
        b.budget as department_budget
    from with_dims e
    left join departments b
        on e.dept_id = b.id
),
with_dims_employees as (
    -- Attach employees attributes
    select e.*, b.salary as employee_salary
    from enriched e
    inner join employees b
        on e.manager_id = b.id
),
joined as (
    -- Attach departments attributes
    select
        src.dept_id,
        src.is_remote,
        src.location,
        src.dept_name,
        src.budget,
        src.department_location,
        src.employee_salary,
        p.budget as department_budget_2,
        p.dept_name as department_dept_name
    from with_dims_employees src
    inner join departments p
        on src.dept_id = p.id
),
enriched_employees as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over w_ordered as employee_salary_percent_rank,
        rank() over (order by department_budget_2 desc nulls last) as department_budget_2_rank,
        percent_rank() over w_recent as department_budget_2_percent_rank
    from joined
    window w_ordered as (order by employee_salary desc nulls last), w_recent as (partition by location order by department_budget_2 desc nulls first)
)
select
    is_remote,
    location,
    dept_name,
    budget,
    department_location,
    employee_salary,
    department_budget_2,
    employee_salary_percent_rank,
    department_budget_2_rank
from enriched_employees;

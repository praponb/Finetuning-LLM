with enriched as (
    select
        a.*,
        d.budget,
        d.dept_name
    from employees a
    left join departments d
        on a.dept_id = d.id
    where d.budget between 2325000 and 3035000
),
enriched_employees as (
    select
        e.id,
        e.manager_id,
        e.job_title,
        e.hire_date,
        e.is_remote,
        e.level,
        e.budget,
        d.hire_date as employee_hire_date,
        d.level as employee_level,
        d.salary as employee_salary
    from enriched e
    left join employees d
        on e.manager_id = d.id
),
joined as (
    select
        src.*,
        b.email,
        b.level as employee_level_2
    from enriched_employees src
    left join employees b
        on src.manager_id <=> b.id
),
enriched_employees_2 as (
    select
        a.id,
        a.manager_id,
        a.job_title,
        a.hire_date,
        a.budget,
        a.employee_hire_date,
        a.employee_level,
        a.employee_salary,
        a.employee_level_2,
        d.level as employee_level_3,
        d.hire_date as employee_hire_date_2
    from joined a
    left join employees d
        on a.manager_id = d.id
)
select
    manager_id,
    approx_percentile(budget, array(0.25, 0.5, 0.75)) as budget_quartiles,
    count(*) as cnt,
    count_if(employee_level = 'L1') as l1_events,
    count(*) as num_rows,
    round(avg(budget), 2) as avg_budget
from enriched_employees_2
group by manager_id
having l1_events < 1000
order by manager_id desc;

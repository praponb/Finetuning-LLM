with with_dims as (
    -- Attach departments attributes
    select
        src.*,
        d.location,
        d.dept_name,
        d.budget
    from employees src
    left join departments d
        on src.dept_id = d.id
),
joined as (
    -- Attach employees attributes
    select
        s.*,
        d.email as employee_email,
        d.salary as employee_salary
    from with_dims s
    left join employees d
        on s.manager_id = d.id
),
enriched as (
    -- Ranking and gap features
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
        location,
        dept_name,
        budget,
        employee_email,
        employee_salary,
        round(employee_salary / sum(employee_salary) over (partition by job_title), 4) as employee_salary_share,
        lead(salary, 2) over (partition by dept_id order by salary, id) as next_salary
    from joined
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, rank() over (partition by job_title order by employee_salary_share nulls last) as rank_in_group
        from enriched
    ) t
    where rank_in_group = 1
)
-- Native SQL PIVOT
select *
from (
    select
        level,
        location,
        job_title,
        budget
    from deduped
) src
pivot (
    max(budget) as amount,
    count(1) as item_count
    for job_title in ('Designer' as designer, 'Intern' as intern)
)

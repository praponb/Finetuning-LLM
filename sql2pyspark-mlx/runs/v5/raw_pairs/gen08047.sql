with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        cast(is_remote as int) is_remote_flag,
        coalesce(level, 'OTHER') level_filled
    from employees
    where hire_date between '2024-03-04' and '2024-04-01'
),
enriched as (
    -- Ranking and gap features
    select
        *,
        lag(salary) over (partition by job_title order by hire_date, id) prev_salary,
        lead(is_remote_flag) over (partition by job_title order by hire_date, id) next_is_remote_flag,
        row_number() over (partition by job_title order by hire_date, id) row_seq,
        lead(salary, 1, 0) over (order by is_remote_flag, id) next_salary
    from cleaned
    where manager_id in (5, 15, 21)
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by manager_id order by next_is_remote_flag desc, id) as latest_rank
    from enriched
    qualify latest_rank = 1
),
ranked_employees as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, rank() over (partition by level, dept_id order by row_seq nulls last) as dedup_rank
        from ranked
    ) t
    where dedup_rank = 1
)
select
    s.id,
    s.email,
    s.dept_id,
    s.manager_id,
    s.hire_date,
    s.level,
    s.is_remote_flag,
    s.level_filled,
    s.prev_salary,
    (select avg(x.salary) from employees x where x.dept_id = s.dept_id) as avg_employee_salary
from ranked_employees s;

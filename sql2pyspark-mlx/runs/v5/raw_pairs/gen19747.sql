with joined as (
    -- Attach employees attributes
    select
        e.*,
        p.salary as employee_salary,
        p.level as employee_level
    from employees e
    join employees p
        on e.manager_id = p.id
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        full_name,
        email,
        level,
        lag(salary) over w_recent as prev_salary,
        lead(salary) over w_recent as next_salary,
        lag(employee_salary) over w_recent as prev_employee_salary,
        lead(employee_salary) over w_recent as next_employee_salary,
        row_number() over w_recent as row_seq,
        sum(salary) over (order by salary, id rows between unbounded preceding and current row) as cumulative_salary
    from joined
    window w_recent as (partition by manager_id order by hire_date, id)
),
latest as (
    -- QUALIFY-style window filter
    select
        id,
        full_name,
        level,
        next_salary,
        row_seq,
        cumulative_salary
    from (
        select *, row_number() over (partition by level order by row_seq nulls last, id) as dedup_rank
        from enriched
    ) x
    where dedup_rank = 1
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        level,
        next_salary,
        cumulative_salary
    from latest
    qualify row_number() over (partition by level order by row_seq desc, id) = 1
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by level order by next_salary nulls first, id) as row_num
    from deduped
    qualify row_num = 1
)
select
    id,
    level,
    next_salary,
    cumulative_salary,
    row_num
from ranked
order by level desc

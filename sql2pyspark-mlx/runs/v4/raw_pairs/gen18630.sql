with joined as (
    -- Attach departments attributes
    select a.*, d.budget
    from employees a
    left join departments d
        on a.dept_id = d.id
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (order by hire_date, id) as row_seq,
        first_value(job_title, true) over (partition by level order by salary, id rows between unbounded preceding and current row) as first_known_job_title
    from joined
),
enriched as (
    -- Attach departments attributes
    select
        e.*,
        d.location,
        d.budget as department_budget,
        d.dept_name
    from scored e
    join departments d
        on e.dept_id = d.id
),
remaining_keys as (
    -- Set intersection on keys
    select id
    from enriched
    intersect
    select id
    from employees
    where hire_date >= '2025-10-01'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    join remaining_keys k on s.id = k.id
),
by_type as (
    -- Native SQL PIVOT
    select *
    from (
        select
            dept_id,
            location,
            department_budget
        from kept_rows
    ) src
    pivot (
        max(department_budget) as value_sum
        for location in ('Singapore', 'Bangkok')
    )
)
select dept_id
from by_type;

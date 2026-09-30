with base as (
    select *, cast(budget as int) as budget_int
    from departments
    where dept_name is not null
),
scored as (
    select
        id,
        dept_name,
        location,
        budget,
        budget_int,
        lag(budget, 1) over w_grp as prev_budget,
        sum(budget) over (partition by dept_name order by budget_int, id rows between unbounded preceding and current row) as running_budget
    from base
    window w_grp as (partition by dept_name, location order by budget, id)
),
metrics as (
    select
        id,
        dept_name,
        location,
        budget,
        budget_int,
        prev_budget,
        running_budget,
        min(prev_budget) over w_running as dept_name_min_prev_budget,
        row_number() over w_hist as event_seq
    from scored
    window w_running as (partition by dept_name), w_hist as (partition by location, dept_name order by budget, id)
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by dept_name, location order by budget_int desc nulls first, id) as rn
        from metrics
    ) x
    where rn = 1
),
kept_keys as (
    select location, dept_name
    from ranked
    except
    select location, dept_name
    from departments
    where location in ('London', 'Chiang Mai')
),
verified as (
    select s.*
    from ranked s
    inner join kept_keys k on s.location = k.location and s.dept_name = k.dept_name
),
kept as (
    select s.*
    from verified s
    where not exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.level in ('L3', 'L5')
        )
),
base_departments as (
    select
        dept_name,
        location,
        budget_int,
        prev_budget,
        running_budget,
        event_seq,
        rn,
        upper(dept_name) as dept_name_upper,
        coalesce(dept_name, 'N/A') as resolved_dept_name
    from kept
    where budget between 2081000 and 4018000
)
select
    dept_name,
    budget_int,
    prev_budget,
    running_budget,
    event_seq,
    rn,
    dept_name_upper,
    resolved_dept_name
from base_departments
order by budget_int desc nulls last, prev_budget desc

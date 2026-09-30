with metrics as (
    -- Running and moving aggregates
    select
        *,
        sum(salary) over (partition by manager_id order by salary, id rows between unbounded preceding and current row) as cumulative_salary,
        lead(salary, 1, 0) over (order by salary, id) as next_salary
    from employees
    where manager_id = 26
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by job_title order by cumulative_salary nulls first, id) as rank_in_group
    from metrics
    qualify rank_in_group = 1
),
scored as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over (partition by dept_id order by cumulative_salary nulls first) as cumulative_salary_percent_rank,
        lead(cumulative_salary, 2) over w_part as next_cumulative_salary,
        ntile(3) over w_hist as salary_bucket,
        avg(salary) over w_hist_2 as salary_avg_3,
        max(next_salary) over (partition by level) as level_max_next_salary
    from ranked
    window w_part as (partition by job_title, manager_id order by cumulative_salary, id), w_hist as (partition by job_title, dept_id order by salary desc, id), w_hist_2 as (partition by manager_id order by next_salary, id rows between 3 preceding and current row)
),
remaining_keys as (
    -- Set intersection on keys
    select manager_id
    from scored
    intersect all
    select manager_id
    from employees
    where level = 'L2'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    inner join remaining_keys k on s.manager_id = k.manager_id
),
with_windows as (
    -- Ranking and gap features
    select *, rank() over (order by cumulative_salary_percent_rank desc nulls first) as cumulative_salary_percent_rank_rank
    from reconciled
)
select
    full_name,
    email,
    dept_id,
    manager_id,
    hire_date,
    level,
    cumulative_salary,
    next_salary,
    rank_in_group
from with_windows
order by rank_in_group;

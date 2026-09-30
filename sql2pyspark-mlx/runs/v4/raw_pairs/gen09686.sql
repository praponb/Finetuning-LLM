with valid as (
    -- Basic cleansing and defaults
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        salary,
        hire_date,
        level,
        concat(email, ' ', level) as email_level,
        round(salary * 1.07, 2) as salary_scaled,
        coalesce(level, 'UNKNOWN') as level_filled
    from employees
    where job_title in ('Intern', 'Director')
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        lag(salary_scaled, 1) over (order by salary_scaled, id) as prev_salary_scaled,
        last_value(salary_scaled) ignore nulls over w_ordered as last_known_salary_scaled,
        rank() over w_ordered_2 as salary_rank,
        max(salary_scaled) over (partition by level) as level_max_salary_scaled,
        first_value(level, true) over (partition by level, dept_id order by salary_scaled, id rows between unbounded preceding and current row) as first_known_level
    from valid
    window w_ordered as (order by salary_scaled, id rows between unbounded preceding and current row), w_ordered_2 as (order by salary desc)
),
metrics_employees as (
    -- Running and moving aggregates
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        salary,
        hire_date,
        level,
        email_level,
        salary_scaled,
        level_filled,
        prev_salary_scaled,
        last_known_salary_scaled,
        salary_rank,
        level_max_salary_scaled,
        first_known_level,
        avg(salary) over (order by salary_scaled, id rows between 5 preceding and current row) as salary_avg_5,
        row_number() over (partition by dept_id, manager_id order by salary_rank desc, id) as event_seq
    from metrics
),
kept_keys as (
    -- Set intersection on keys
    select level
    from metrics_employees
    intersect
    select level
    from employees
    where job_title = 'Designer'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics_employees s
    join kept_keys k on s.level = k.level
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        round(event_seq / sum(event_seq) over (partition by manager_id, dept_id), 4) as event_seq_share,
        dense_rank() over (order by salary desc) as salary_dense_rank
    from verified
)
-- Native SQL PIVOT
select *
from (
    select
        manager_id,
        dept_id,
        level,
        level_max_salary_scaled
    from enriched
) src
pivot (
    avg(level_max_salary_scaled)
    for level in ('L4', 'L5')
);

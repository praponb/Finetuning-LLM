with filtered as (
    -- Keep recent, valid rows
    select
        *,
        concat_ws(',', level, full_name) as level_full_name_key,
        if(is_remote, 'Yes', 'No') as is_remote_label,
        coalesce(job_title, 'NONE') as resolved_job_title
    from employees
    where hire_date > date '2023-02-24'
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        sum(salary) over w_ordered as cumulative_salary,
        lag(salary, 1) over (partition by dept_id, level order by hire_date, id) as prev_salary,
        row_number() over (partition by dept_id order by salary desc, id) as row_seq
    from filtered
    where email is not null
    window w_ordered as (partition by level order by hire_date, id rows between unbounded preceding and current row)
),
deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        email,
        dept_id,
        manager_id,
        salary,
        hire_date,
        level,
        is_remote_label,
        cumulative_salary
    from (
        select *, row_number() over (partition by job_title, level order by salary desc, id) as dedup_rank
        from metrics
    ) x
    where dedup_rank = 1
),
with_dims as (
    -- Attach departments attributes
    select e.*, p.location
    from deduped e
    inner join departments p
        on e.dept_id = p.id
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by level order by cumulative_salary desc, id) as rank_in_group
    from with_dims
    qualify rank_in_group = 1
),
matched_keys as (
    -- Set difference on keys
    select level, dept_id
    from latest
    except
    select level, dept_id
    from employees
    where dept_id in (10, 11, 13, 24)
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest s
    inner join matched_keys k on s.level = k.level and s.dept_id = k.dept_id
)
select
    id,
    email,
    dept_id,
    manager_id,
    salary,
    hire_date,
    is_remote_label,
    cumulative_salary,
    location
from reconciled
order by hire_date, dept_id;

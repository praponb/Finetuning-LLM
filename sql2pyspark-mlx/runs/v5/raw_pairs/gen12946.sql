with with_dims as (
    -- Attach departments attributes
    select
        src.*,
        d.budget,
        d.dept_name,
        d.location
    from employees src
    join departments d
        on src.dept_id = d.id
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        avg(salary) over (partition by dept_id) as dept_id_avg_salary,
        first_value(location) ignore nulls over (partition by job_title order by salary, id rows between unbounded preceding and current row) as first_known_location,
        ntile(3) over w_recent as salary_bucket,
        ntile(4) over (partition by dept_name, level order by salary, id) as salary_bucket_2
    from with_dims
    where level = 'L1'
    window w_recent as (order by salary desc, id)
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by dept_name order by dept_id_avg_salary desc nulls first, id) as dedup_rank
    from metrics
    qualify dedup_rank = 1
),
joined as (
    -- Attach departments attributes
    select
        src.*,
        p.location as department_location,
        p.dept_name as department_dept_name
    from deduped src
    left join departments p
        on src.dept_id = p.id
)
-- Running and moving aggregates
select
    id,
    email,
    dept_id,
    manager_id,
    job_title,
    salary,
    is_remote,
    level,
    dept_name,
    location,
    first_known_location,
    salary_bucket_2,
    dedup_rank,
    department_location,
    department_dept_name,
    round(salary / sum(salary) over (partition by dept_id, job_title), 4) as salary_share,
    last_value(department_location, true) over (partition by location, manager_id order by dedup_rank, id rows between unbounded preceding and current row) as last_known_department_location,
    ntile(5) over w_ordered as salary_bucket_2_bucket
from joined
window w_ordered as (partition by location, department_location order by salary_bucket_2, id);

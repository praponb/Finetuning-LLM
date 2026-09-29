with enriched as (
    -- Window metrics over several different partitions
    select
        *,
        lead(salary) over (partition by dept_id, level order by salary, id) as next_salary,
        row_number() over (partition by dept_id, level order by salary, id) as seq_no
    from employees
    where is_remote
),
matched_keys as (
    -- Set intersection on keys
    select level, id
    from enriched
    intersect all
    select level, id
    from employees
    where dept_id in (22, 32, 33)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join matched_keys k on s.level = k.level and s.id = k.id
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        next_salary,
        seq_no,
        case when is_remote then 'Yes' else 'No' end as is_remote_label
    from verified
    where hire_date > to_date('2023-09-07')
      and level is not null
),
joined as (
    -- Attach employees attributes
    select
        s.*,
        b.level as employee_level,
        b.salary as employee_salary
    from valid s
    left join employees b
        on s.manager_id = b.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (partition by job_title, dept_id order by next_salary desc nulls last) as next_salary_dense_rank,
        stddev_samp(employee_salary) over (partition by manager_id, job_title) as manager_id_stddev_samp_employee_salary
    from joined
),
joined_employees as (
    -- Attach departments attributes
    select
        src.email,
        src.dept_id,
        src.manager_id,
        src.job_title,
        src.salary,
        src.hire_date,
        src.is_remote,
        src.seq_no,
        src.is_remote_label,
        src.employee_salary,
        src.next_salary_dense_rank,
        src.manager_id_stddev_samp_employee_salary,
        p.dept_name
    from with_windows src
    left join departments p
        on src.dept_id = p.id
),
valid_employees as (
    -- Basic cleansing and defaults
    select
        manager_id,
        job_title,
        salary,
        hire_date,
        seq_no,
        is_remote_label,
        next_salary_dense_rank,
        round(manager_id_stddev_samp_employee_salary, 0) as manager_id_stddev_samp_employee_salary_rounded
    from joined_employees
    where manager_id_stddev_samp_employee_salary is not null
      and dept_name is not null
)
-- Grouped metrics
select
    date_trunc('year', hire_date) as year_start,
    case when seq_no >= 8 then 'LARGE' else 'SMALL' end as seq_no_tier,
    job_title,
    count(*) filter (where job_title = 'Designer') as designer_rows,
    approx_percentile(manager_id_stddev_samp_employee_salary_rounded, array(0.25, 0.5, 0.75)) as manager_id_stddev_samp_employee_salary_rounded_quartiles,
    round(avg(salary), 2) as avg_salary
from valid_employees
group by date_trunc('year', hire_date), case when seq_no >= 8 then 'LARGE' else 'SMALL' end, job_title
having count(*) filter (where job_title = 'Designer') < 1000
order by year_start nulls first;

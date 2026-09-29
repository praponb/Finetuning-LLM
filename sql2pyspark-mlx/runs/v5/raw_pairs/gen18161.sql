with base as (
    -- Filter the raw rows and derive helper columns
    select *, substring(full_name, 1, 4) as full_name_prefix
    from employees
    where level is not null
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        full_name,
        dept_id,
        job_title,
        salary,
        is_remote,
        level,
        max(salary) over (partition by dept_id, job_title) as dept_id_max_salary,
        first_value(job_title) ignore nulls over (partition by job_title order by salary, id rows between unbounded preceding and current row) as first_known_job_title,
        row_number() over (partition by dept_id, job_title order by salary, id) as seq_no
    from base
    where month(hire_date) in (1, 3, 5)
),
metrics_employees as (
    -- Running and moving aggregates
    select
        *,
        lag(seq_no) over (partition by job_title order by salary, id) as prev_seq_no,
        lag(salary) over (partition by job_title order by salary, id) as prev_salary,
        row_number() over (partition by job_title order by salary, id) as event_seq
    from metrics
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        dept_id,
        salary,
        is_remote,
        dept_id_max_salary,
        first_known_job_title,
        prev_seq_no,
        prev_salary,
        count(prev_salary) over w_ordered as last7_count_prev_salary,
        max(prev_salary) over w_ordered as last7_max_prev_salary,
        min(prev_salary) over w_ordered as last7_min_prev_salary
    from metrics_employees
    window w_ordered as (partition by job_title, dept_id order by prev_salary, id rows between 6 preceding and current row)
),
metrics_employees_2 as (
    -- Ranking and gap features
    select
        *,
        lead(prev_seq_no) over w_hist as next_prev_seq_no,
        lag(last7_min_prev_salary) over w_hist as prev_last7_min_prev_salary,
        row_number() over w_hist as seq_no,
        lead(salary, 1) over (order by salary, id) as next_salary
    from scored
    window w_hist as (partition by dept_id order by salary, id)
)
select
    s.id,
    s.dept_id,
    s.salary,
    s.dept_id_max_salary,
    s.prev_seq_no,
    s.prev_salary,
    s.last7_max_prev_salary,
    s.last7_min_prev_salary,
    s.next_prev_seq_no,
    (select max(x.salary) from employees x where x.dept_id = s.dept_id) as max_employee_salary
from metrics_employees_2 s
order by last7_max_prev_salary desc;

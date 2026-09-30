with valid as (
    -- Keep recent, valid rows
    select
        *,
        datediff(current_date(), hire_date) as days_since_hire_date,
        length(job_title) as job_title_length,
        coalesce(job_title, 'N/A') as job_title_clean
    from employees
    where hire_date > to_date('2023-08-12')
      and manager_id is not null
),
scored as (
    -- Running and moving aggregates
    select
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary,
        hire_date,
        level,
        job_title_length,
        sum(salary) over w_running as running_sum_salary,
        count(salary) over w_running as running_count_salary,
        min(salary) over w_running as running_min_salary,
        row_number() over (partition by dept_id order by hire_date, id) as seq_no
    from valid
    window w_running as (partition by level, manager_id order by hire_date, id rows between unbounded preceding and current row)
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary,
        level,
        running_count_salary,
        seq_no,
        avg(job_title_length) over (partition by manager_id) as manager_id_avg_job_title_length,
        ntile(3) over (partition by job_title order by running_min_salary desc, id) as running_min_salary_bucket,
        ntile(10) over (partition by job_title order by running_min_salary desc, id) as running_min_salary_bucket_2
    from scored
),
metrics_employees as (
    -- Ranking and gap features
    select
        id,
        email,
        salary,
        level,
        seq_no,
        manager_id_avg_job_title_length,
        running_min_salary_bucket,
        running_min_salary_bucket_2,
        lag(manager_id_avg_job_title_length) over (partition by manager_id order by seq_no, id) as prev_manager_id_avg_job_title_length,
        lead(manager_id_avg_job_title_length) over (partition by manager_id order by seq_no, id) as next_manager_id_avg_job_title_length,
        lag(seq_no) over (partition by manager_id order by seq_no, id) as prev_seq_no,
        lead(seq_no) over (partition by manager_id order by seq_no, id) as next_seq_no,
        row_number() over (partition by manager_id order by seq_no, id) as row_seq
    from metrics
    where running_min_salary_bucket >= 4
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by level order by running_min_salary_bucket desc nulls first, id) as dedup_rank
        from metrics_employees
    ) ranked
    where dedup_rank = 1
)
-- Running and moving aggregates
select
    id,
    email,
    salary,
    running_min_salary_bucket_2,
    next_manager_id_avg_job_title_length,
    next_seq_no,
    row_seq,
    ntile(3) over (partition by level order by seq_no, id) as seq_no_bucket,
    avg(row_seq) over (order by seq_no, id rows between 5 preceding and 1 preceding) as row_seq_avg_5_prior,
    sum(running_min_salary_bucket_2) over (partition by level order by prev_manager_id_avg_job_title_length, id rows between unbounded preceding and current row) as cumulative_running_min_salary_bucket_2
from deduped;

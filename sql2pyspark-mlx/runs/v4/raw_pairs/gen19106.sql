with cleaned as (
    select *, upper(trim(email)) as email_normalized
    from employees
    where salary > 148000
),
enriched as (
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        email_normalized,
        row_number() over (order by salary, id) as event_seq,
        row_number() over w_part as seq_no,
        sum(salary) over (partition by dept_id order by salary, id rows between unbounded preceding and current row) as cumulative_salary
    from cleaned
    window w_part as (partition by job_title order by hire_date desc, id)
),
with_dims as (
    select
        e.*,
        d.job_title as employee_job_title,
        d.full_name as employee_full_name
    from enriched e
    left join employees d
        on e.manager_id = d.id
),
base as (
    select
        id,
        full_name,
        email,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        event_seq,
        seq_no,
        cumulative_salary,
        employee_job_title,
        employee_full_name,
        greatest(cumulative_salary, seq_no) as greatest_cumulative_salary_seq_no,
        concat_ws(',', email, full_name) as email_full_name_key
    from with_dims
    where hire_date >= date '2023-05-28'
      and level != 'L3'
)
select
    date_trunc('week', hire_date) as week_start,
    max(event_seq) as max_event_seq,
    avg(seq_no) filter (where employee_job_title = 'Analyst') as analyst_avg_seq_no,
    sum(case when level = 'L5' then event_seq else 0 end) as l5_event_seq,
    count(distinct job_title) as distinct_job_titles,
    count(*) filter (where level = 'L2') as l2_rows,
    count_if(job_title = 'Engineer') as engineer_events
from base
group by date_trunc('week', hire_date)
order by engineer_events desc

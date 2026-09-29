with matched_keys as (
    select id
    from employees
    except all
    select id
    from employees
    where level in ('L1', 'L3')
),
kept_rows as (
    select s.*
    from employees s
    inner join matched_keys k on s.id = k.id
)
select
    job_title,
    date_trunc('week', hire_date) as week_start,
    manager_id,
    count(distinct id) as distinct_ids,
    approx_percentile(salary, array(0.25, 0.5, 0.75)) as salary_quartiles,
    count_if(level = 'L5') as l5_events
from kept_rows
group by cube (job_title, date_trunc('week', hire_date), manager_id)
order by manager_id desc;

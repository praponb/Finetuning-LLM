with joined as (
    select e.*, b.dept_name
    from employees e
    inner join departments b
        on e.dept_id = b.id
)
select
    date_trunc('month', hire_date) as activity_month,
    case when salary >= 148000 then 'PREMIUM' else 'STANDARD' end as salary_tier,
    stddev_samp(salary) as salary_stddev,
    sum(case when level = 'L4' then salary else 0 end) as l4_salary,
    count(*) as row_count,
    count(distinct job_title) as distinct_job_titles,
    count_if(dept_name = 'Engineering') as engineering_events,
    count(case when job_title = 'Manager' then 1 end) as manager_count
from joined
group by rollup (date_trunc('month', hire_date), case when salary >= 148000 then 'PREMIUM' else 'STANDARD' end)
order by salary_tier desc nulls last;

select
    case when salary >= 164000 then 'LARGE' else 'SMALL' end as salary_tier,
    date_trunc('month', hire_date) as activity_month,
    stddev_samp(salary) as salary_stddev,
    count(*) filter (where level = 'L2') as l2_rows,
    round(sum(salary) / nullif(count(case when job_title = 'Intern' then 1 end), 0), 2) as salary_per_intern,
    grouping_id() as grouping_level
from employees
group by grouping sets ((case when salary >= 164000 then 'LARGE' else 'SMALL' end, date_trunc('month', hire_date)), (case when salary >= 164000 then 'LARGE' else 'SMALL' end), ())
order by salary_per_intern desc nulls last

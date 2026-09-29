select
    case when salary >= 130000 then 'LARGE' else 'SMALL' end as salary_tier,
    max(salary) filter (where level = 'L4') as l4_max_salary,
    sort_array(collect_set(job_title)) as job_title_list,
    min(salary) as min_salary,
    stddev_samp(salary) as salary_stddev
from employees
group by 1;

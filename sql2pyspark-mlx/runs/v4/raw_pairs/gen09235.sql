select
    date_trunc('year', hire_date) as year_start,
    max(salary) filter (where job_title = 'Manager') as manager_max_salary,
    count(distinct level) as distinct_levels,
    sum(case when level = 'L4' then salary else 0 end) as l4_salary,
    round(sum(salary) / nullif(count(case when job_title = 'Engineer' then 1 end), 0), 2) as salary_per_engineer,
    round(avg(salary), 2) as avg_salary,
    round(avg(salary), 2) as avg_salary_2
from employees
group by rollup (date_trunc('year', hire_date))
having avg_salary_2 > 0;

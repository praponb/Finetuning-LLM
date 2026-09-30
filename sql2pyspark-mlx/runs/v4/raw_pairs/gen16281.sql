select job_title, round(stddev(salary), 2) as stddev_salary
from employees
where not (full_name like '%Olivia%' and email like '%@gmail.com')
group by job_title
having round(stddev(salary), 2) > 40
order by job_title asc, stddev_salary desc;

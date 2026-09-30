select manager_id, count(case when job_title = 'Analyst' then 1 end) analyst_count
from employees
group by manager_id
having analyst_count < 2
order by manager_id

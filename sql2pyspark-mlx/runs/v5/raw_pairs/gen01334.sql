select dept_id, round(stddev(salary), 2) as stddev_salary
from employees
where not is_remote
group by dept_id;

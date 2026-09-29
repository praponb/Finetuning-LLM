select level, max(salary) as max_salary, sum(salary) as total_salary from employees group by level having total_salary > 713000 order by total_salary desc

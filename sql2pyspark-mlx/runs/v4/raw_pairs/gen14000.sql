select job_title, max(salary) - min(salary) salary_range from employees where level = 'L3' or salary > 74000 group by job_title having salary_range >= 38;

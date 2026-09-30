select job_title, sum(salary) as total_salary, round(sum(salary) * 100.0 / sum(sum(salary)) over (), 2) as pct_of_total from employees where level <> 'L3' group by job_title order by 3

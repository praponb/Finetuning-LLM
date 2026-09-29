select id, dept_id, salary, percent_rank() over (partition by dept_id order by salary desc) as salary_pct_rank from employees order by dept_id desc;

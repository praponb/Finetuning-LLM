select id, dept_id, hire_date, dense_rank() over (partition by dept_id order by hire_date) as dense_rnk from employees order by hire_date desc;

select
    id,
    dept_id,
    level,
    hire_date,
    dense_rank() over (partition by dept_id, level order by hire_date) as dense_rnk
from employees
where email like '%@yahoo.com'

select
    id,
    level,
    salary,
    dense_rank() over (partition by level order by salary desc) as dense_rnk
from employees
where job_title = 'Manager';

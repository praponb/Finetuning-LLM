select
    id,
    level,
    dept_id,
    salary,
    round(salary / sum(salary) over (partition by level, dept_id), 3) as salary_share,
    last_value(dept_id) over (partition by level order by salary desc, id rows between unbounded preceding and unbounded following) as last_dept_id
from employees
order by dept_id desc;

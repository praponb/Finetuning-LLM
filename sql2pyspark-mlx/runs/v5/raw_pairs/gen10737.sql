select
    *,
    row_number() over (partition by manager_id order by salary, id) seq_no,
    first_value(level, true) over w_grp first_known_level,
    cume_dist() over (partition by dept_id order by salary) salary_cume_dist
from employees
where salary between 100000 and 106000
window w_grp as (partition by level order by hire_date, id rows between unbounded preceding and current row);

select id, manager_id
from employees
where dept_id in (8, 19, 29, 39) and salary < 94000;

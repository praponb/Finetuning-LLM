select
    id,
    manager_id,
    email,
    date_format(hire_date, 'yyyy-MM') as hire_date_label,
    date_add(hire_date, 39) as hire_date_plus_39d
from employees
where level != 'L3' and salary between 56000 and 87000;

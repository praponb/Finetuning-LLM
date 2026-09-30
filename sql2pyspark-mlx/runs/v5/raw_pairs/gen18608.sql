select hire_date
from employees
union
select opened_date
from accounts;

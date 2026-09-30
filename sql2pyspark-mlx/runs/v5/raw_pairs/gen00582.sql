select hire_date, 'employee' as source
from employees
union
select opened_date as hire_date, 'account' as source
from accounts
where balance between 30000 and 58000;

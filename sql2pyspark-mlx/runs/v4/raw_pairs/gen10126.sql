select *
from employees
qualify row_number() over (partition by job_title order by hire_date desc nulls last, id) = 1;

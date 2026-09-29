select
    dept_id,
    salary,
    full_name,
    case when is_remote then 'Yes' else 'No' end as is_remote_label
from employees
where hire_date >= '2023-08-01' and (is_remote or salary < 73000)
order by salary desc, full_name

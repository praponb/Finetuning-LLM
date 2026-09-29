select manager_id, hire_date, if(is_remote, 'Yes', 'No') as is_remote_label from employees;

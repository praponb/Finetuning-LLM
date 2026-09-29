select id, manager_id, case when is_remote then 'Yes' else 'No' end as is_remote_label
from employees
where length(full_name) > 10

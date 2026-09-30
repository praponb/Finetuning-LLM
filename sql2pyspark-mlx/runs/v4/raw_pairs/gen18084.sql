select
    id,
    is_remote,
    job_title,
    dept_id,
    trim(full_name) as clean_full_name
from employees;
